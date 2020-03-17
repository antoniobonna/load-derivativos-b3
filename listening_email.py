import telepot
import ezgmail
import emoji
from subprocess import call
from time import sleep

### Carrega emojis
OK = emoji.emojize(':heavy_check_mark:', use_aliases=True)
NOK = emoji.emojize(':cross_mark:', use_aliases=True)

### Ids do Telegram

botID = '784006906:AAF1qZj6fA9HdfdTijq04rmJ8nb5O43bmUg'
channelID = '@datasciencetork'

### variaveis
indir = '/home/ubuntu/scripts/load-dados-bmf/'
bot = telepot.Bot(botID)

print('Listening e-mail...')
while (1):
    threads = ezgmail.unread() ### verifica e-mails marcados como não lidos
    for thread in threads:
        summary = ezgmail.summary(thread)
        sumary = ezgmail.summary(thread, printInfo=False)
        if 'datarestore@b3.com.br' in sumary[0][0][0]: ### verifica se e-mail recebido é da B3
            datetime = str(sumary[0][2])
            bot.sendMessage(channelID,'Novo e-mail recebido da B3 às {} '.format(datetime) + OK)
            attachment = thread.messages[0].attachments[0] ### pega o nome do anexo
            thread.messages[0].downloadAttachment(attachment, indir)
            call('python '+indir+'parseResult.py',shell=True)
            call('python '+indir+'new_writeExcel.py',shell=True)
            call('python '+indir+'sendEmail.py',shell=True)
            exit(0)
    sleep(60)
