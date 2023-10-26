import re
sentance = ''' hello" sir "'''
res = re.sub('[!,*)@#%(&$_?.^"]', '', sentance)
print(res)
