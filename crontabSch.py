from crontab import CronTab

cTab = CronTab(user='root')
task = cTab.new(command='duplicate_removal.py')
task.minute.every(30)

cTab.write()