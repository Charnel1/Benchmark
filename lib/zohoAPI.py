import requests
import json
import os
import pandas as pd
import time


#   https://accounts.zoho.com/oauth/v2/auth?scope=ZohoCRM.modules.ALL&client_id=1000.L2VPCGPSKTWW2K9ZOELCXGGKLYXUHH&response_type=code&access_type=offline&redirect_uri=https://localhost:8000


class APIZohoHandler:

    def __init__(self, _client_id, _client_secret, _code):
        self.client_id = _client_id
        self.client_secret = _client_secret
        self.code = _code
        self.EndTimeConection = 99999999999.99999
        self.Connected = False

    def AccessToken(self):
        url = "https://accounts.zoho.eu/oauth/v2/token"
        data = {
            "grant_type": "authorization_code",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": "https://localhost:8000",
            "code": self.code
        }
        response = requests.post(url, data=data)
        match response.status_code:
            case 200:
                tokens = response.json()
                self.access_token = tokens.get('access_token')
                self.refresh_token = tokens.get('refresh_token')
                self.EndTimeConection = time.time() + 3600
                return self.SaveZohoAccessTokenFile(tokens)
            case _:
                print(response.status_code)
                return False

    def UseGetConnection(self):
        if self.Check_EndTimeConection() == False or self.Connected == False:
            self.refresh_token = self.OpenZohoAccessTokenFile().get('refresh_token')
            _bool, token = self.RefreshToken(self.refresh_token)
            if _bool:
                self.access_token = token.get('access_token')
                self.EndTimeConection = time.time() + 3600
                self.Connected = True
                return True
            else:
                print("ERROR!")
                return False
        return True, "Connection Alive"

    def Check_EndTimeConection(self):
        if time.time() >= self.EndTimeConection:
            self.Connected = False
            return False
        return True

    def SaveZohoAccessTokenFile(self, tokens):
        with open("lib/zoho_token.json", "w") as f:
            d = json.dump(tokens, f)
        return d

    def SaveJsonFile(self, Data):

        with open("lib/zoho_token.json", "w") as f:
            d = json.dump(Data, f)
        return d

    def LoadJsonFile(self):
        with open("lib/zoho_token.json", "r") as f:
            d = json.load(f)
        print(d)
        return d

    def OpenZohoAccessTokenFile(self):
        with open("lib/zoho_token.json", "r") as f:
            d = json.load(f)
        return d

    def RefreshToken(self, _refresh_token):
        url = "https://accounts.zoho.eu/oauth/v2/token"
        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": f"{_refresh_token}"
        }
        response = requests.post(url, data=data)
        tokens = response.json()
        match response.status_code:
            case 200:

                return True, tokens
                # self.SaveZohoAccessTokenFile(tokens)
            case _:
                print(response.status_code)
                return False, None

    def GET_ClientDetails(self, _id):
        url = f"https://www.zohoapis.eu/crm/v2/Accounts/{_id}"

        h = {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=h)

        match response.status_code:
            case 200:
                try:
                    return response.json()
                except:
                    return False
            case _:
                return False

    def GET_OppDetails(self, _id):
        url = f"https://www.zohoapis.eu/crm/v2/Potentials/{_id}"

        h = {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=h)
        match response.status_code:
            case 200:
                try:
                    return response.json()
                except:
                    return False
            case _:
                return False

    def Print_InfoClient(self, data):
        _id = data.get('data')[0].get('id')
        _Kundenname = data.get('data')[0].get('Account_Name')
        _stadt = data.get('data')[0].get('Billing_City')
        _plz = data.get('data')[0].get('Billing_Code')
        _country = data.get('data')[0].get('Billing_Country')
        _strasse = data.get('data')[0].get('Billing_Street')
        return [_Kundenname, _stadt, _plz, _country, _strasse, _id]

    def Print_InfoOpp(self, data):
        # Kunden ID
        _id = data.get('data')[0].get('Account_Name').get('id')

        return

    def AccessCode_URL(self):
        url = "https://accounts.zoho.com/oauth/v2/auth?scope={}&client_id={_client_id}&response_type=JSON&access_type=offline&redirect_url=http://localhost:8000"
        data = {
            "scope": "ZohoCRM.modules.ALL",
            "client_id": self.client_id,
            "response_type": "code",
            "access_type": "offline",
            "client_secret": self.client_secret,
            "redirect_url": "http://localhost:8500"
        }
        response = requests.get(url)
        print(response.status_code)

        match response.status_code:
            case 200:
                tokens = response.json()
                with open("lib/zoho_token.json", "w") as f:
                    json.dump(tokens, f)
                return tokens
            case _:
                print(response.status_code)
                return

    def CheckIdForBothClientAndOpp(self, _ID):
        if self.UseGetConnection() == True:
            result = self.GET_ClientDetails(_ID)
            if result != False:
                return result, "Client"
            else:
                result = self.GET_OppDetails(_ID)
                if result != False:
                    _id = result.get('data')[0].get('Account_Name').get('id')
                    result = self.GET_ClientDetails(_id)
                    return result, "Opportunity"

                print(result)
                return False, "Not Found Anything"
