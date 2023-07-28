import base64
import datetime
import json


def handler(event, context):
    output = []

    for record in event["records"]:
        # decode base64 data
        payload = base64.b64decode(record["data"])
        # convert bytes to string
        payload_str = payload.decode("utf-8")
        # convert string to json
        payload_json = json.loads(payload_str)

        payload_json["season"] = int(payload_json["season"])
        payload_json["innings"] = int(payload_json["innings"])
        payload_json["ball"] = float(payload_json["ball"])
        payload_json["runs_off_bat"] = int(payload_json["runs_off_bat"])
        payload_json["extras"] = int(payload_json["extras"])
        payload_json["score_board"] = (
            payload_json["runs_off_bat"] + payload_json["extras"]
        )
        payload_json["is_dismissal"] = 1 if payload_json["wicket_type"] else 0

        # transform to json string and append a newline
        transformed_payload = json.dumps(payload_json) + "\n"

        # encode transformed payload to base64
        transformed_data = base64.b64encode(transformed_payload.encode("utf-8")).decode(
            "utf-8"
        )

        output_record = {
            "recordId": record["recordId"],
            "result": "Ok",
            "data": transformed_data,
        }
        output.append(output_record)

    return {"records": output}
