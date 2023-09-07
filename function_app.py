import json
import azure.functions as func
import logging
import tiktoken
import re
from azure.functions.decorators.core import DataType
import datetime
import pytz

app = func.FunctionApp()

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="tkstream",
                               connection="evtkfk_sendlisten_EVENTHUB") 
@app.generic_output_binding(arg_name="gptlogs", type="sql", 
                            CommandText="[dbo].[tblGPTLogs]",
                            ConnectionStringSetting="GPTLOGS_SQLDB",
                            data_type=DataType.STRING
                            )
def tkcount(azeventhub: func.EventHubEvent, gptlogs: func.Out[func.SqlRow]):
    
    msgbody = azeventhub.get_body().decode('utf-8')
    # logging.info('Python EventHub trigger processed an event: %s',
    #             azeventhub.get_body().decode('utf-8'))
    msgjson = json.loads(msgbody)
    reqbody = msgjson.get('RequestBody')
    logging.debug('request body: %s', reqbody)
    resbody = msgjson.get('ResponseBody')
    logging.debug('response body: %s', resbody)
    newResp = { 
        "messages": "",
        "model": "",
        "usage":{
            "completion_tokens": 0,
            "prompt_tokens": 0,
            "total_tokens": 0
        }
    }
    enc = tiktoken.encoding_for_model("gpt-3.5-turbo")
    usage = newResp["usage"]
    #count completion tokens
    if resbody is not None:
        start_index = resbody.find('"model":') + len('"model":"')  # find the index of the start of the value of "model"
        end_index = resbody.find('",', start_index)  # find the index of the closing quote after the value of "model"
        model = resbody[start_index:end_index]
        logging.info('%d, %d,model: %s', start_index, end_index, model)
        
        newResp["model"] = model

        delta_contents = re.findall(r'"delta":{"content":"(.*?)"', resbody)
        result = ''.join(delta_contents)
        newResp["messages"] = result
        
        encoded = enc.encode(result)
        usage["completion_tokens"] = len(encoded)

        #count prompt tokens
        if reqbody is not None:
            reqmsg = reqbody["messages"]
            if reqmsg is not None:
                content_list = [msg.get('content') for msg in reqmsg]
                content_str = ''.join(content_list)
                logging.info('content: %s', content_str)
                encoded = enc.encode(content_str)
                usage["prompt_tokens"] = len(encoded) + 11 #11 is the length of the prompt header

    usage["total_tokens"] = usage["prompt_tokens"] + usage["completion_tokens"]
    logging.info('new response: %s', newResp)

    body = json.loads(msgbody)
    body["ResponseBody"] = str(json.dumps(newResp))
    body["RequestBody"] = str(reqbody)

    EventTime = msgjson.get('EventTime')
    EventTime = EventTime[:26] + EventTime[26:].replace(EventTime[26], '', 1)
    EventTime = datetime.datetime.fromisoformat(EventTime.replace('Z', '+00:00')).astimezone(pytz.timezone('Asia/Shanghai'))

    body["EventTime"] = EventTime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    logging.info('new body: %s', body)
    row = func.SqlRow(body)
    gptlogs.set(row)



    

    
