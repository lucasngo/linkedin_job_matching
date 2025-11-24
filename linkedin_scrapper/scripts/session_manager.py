import sys
sys.path.append('../')
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import requests
import pandas as pd


class SessionManager:
    def __init__(self, BROWSER = 'chrome'):
        self.BROWSER = BROWSER
        self.session_index = 0
        self.sessions = self.get_sessions('search')
        self.emails = None
        self.passwords = None
 
    def create_session(self,email, password):
        if self.BROWSER == 'chrome':
            driver = webdriver.Chrome()
        elif self.BROWSER == 'edge':
            driver = webdriver.Edge()

        driver.get('https://www.linkedin.com/checkpoint/rm/sign-in-another-account')
        time.sleep(1)
        driver.find_element(By.ID, 'username').send_keys(email)
        driver.find_element(By.ID, 'password').send_keys(password)
        driver.find_element(By.XPATH, "//button[contains(text(),'Sign in')]").click()
        time.sleep(1)
        input('Press ENTER after a successful login for "{}": '.format(email))
        driver.get('https://www.linkedin.com/jobs/search/?')
        time.sleep(1)
        cookies = driver.get_cookies()
        driver.quit()
        session = requests.Session()
        for cookie in cookies:
            session.cookies.set(cookie['name'], cookie['value'])
        return session

    def get_logins(self,method):
        logins = pd.read_csv('../logins.csv')
        logins = logins[logins['method'] == method]
        emails = logins['emails'].tolist()
        passwords = logins['passwords'].tolist()
        return emails, passwords

    def get_sessions(self,method):
        self.emails, self.passwords = self.get_logins(method)
        sessions = [self.create_session(email, password) for email, password in zip(self.emails, self.passwords)]
        return sessions
