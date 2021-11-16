import json

CREDS_PATH = "  "


class TwilioCredsUpdator:
    def __init__(self, account_name: str):
        if self.check_validity(account_name.upper()):
            self.account_name = account_name.upper()
            self.creds = self.get_all_creds()
        else:
            raise Exception("ACCOUNT NAME DOESNT EXISTS")

    def get_all_creds(self):
        with open(CREDS_PATH, "r") as file:
            return json.load(file)

    def check_validity(self, account_name):
        with open(CREDS_PATH, "r") as file:
            res = json.load(file)
            if account_name in list(res.keys()):
                return True
            else:
                return False

    def update_sid(self, new_sid):
        self.creds[self.account_name]["SID"] = new_sid
        return self

    def update_auth(self, new_auth):
        self.creds[self.account_name]["AUTH"] = new_auth
        return self

    def update_registered_number(self, new_number):
        self.creds[self.account_name]["NUMBER"] = new_number
        return self

    def flush_all_updates(self):
        with open(CREDS_PATH, "w") as file:
            file.write(json.dumps(self.creds))
            print("UPDATED")


class TwilioCreds:
    def __init__(self):
        self.credentials = {}

    def load_creds(self, account_name="PAID"):
        self.account_name = account_name
        with open(CREDS_PATH, "r") as cred:
            res = json.load(cred)
            self.credentials = res[self.account_name]
            return self

    def get_sid(self):
        return self.credentials["SID"]

    def get_auth_token(self):
        return self.credentials["AUTH"]

    def get_registered_number(self):
        return self.credentials["NUMBER"]

    def add_creds(self, account_name: str, sid: str, auth: str, number: str):
        account_name = account_name.upper()
        with open(CREDS_PATH, "r") as cred:
            res = json.load(cred)
            if account_name not in list(res.keys()):
                res[account_name] = {
                    "SID": sid,
                    "AUTH": auth,
                    "NUMBER": number
                }
                with open(CREDS_PATH, "w") as add_creds:
                    add_creds.write(json.dumps(res))
                    print("CREDS ADDED")
            else:
                raise Exception("Creds Already Exists")

    def update_creds(self):
        return TwilioCredsUpdator(self.account_name)

    def delete_creds(self):
        """
        Deletes the current account_name creds
        :return:
        """
        with open(CREDS_PATH, "r") as read_file:
            res = json.load(read_file)
            res.pop(self.account_name)
            with open(CREDS_PATH, "w") as write_file:
                write_file.write(json.dumps(res))
                print(f"{self.account_name} has been deleted")
