import yagmail
import os
import credentials
from datetime import date,datetime

GMAIL_USERNAME, GMAIL_PASSWORD = credentials.setEmailLogin()
SENDER_EMAIL = 'marruda@torkcapital.com.br' #marruda@torkcapital.com.br

file = 'new_BMF_SOTP.xlsx'
indir = '/home/ubuntu/scripts/load-dados-bmf/'
current_day = date.today().strftime('%d/%m/%y')

def greeting():
    hour = datetime.now().hour
    if hour < 12:
        greeting = 'Bom dia'
    elif hour < 19:
        greeting = 'Boa tarde'
    else:
        greeting = 'Boa noite'
    return greeting

yag = yagmail.SMTP(GMAIL_USERNAME, GMAIL_PASSWORD)

begin_message = '<font color="#1f497d"><p>{} Murilo,</p><p>Segue em anexo o c&aacute;lculo dos derivativos da B3.</p><p>Att,</p><p>Antonio</p></font>'.format(greeting())
yag.send(to = SENDER_EMAIL, subject = 'Dados Derivativos - ' + current_day, contents = [begin_message, indir+file])
os.remove(indir+file)