import xfuncs
import fuzzy
import cyrtranslit

dmeta = fuzzy.DMetaphone()
print(dmeta(cyrtranslit.to_latin(u"Дегтярёв",'ru')))
print(dmeta(cyrtranslit.to_latin(u"Диктерев",'ru')))

print(fuzzy.nysiis(cyrtranslit.to_latin(u"Дегтярёв",'ru')))
print(fuzzy.nysiis(cyrtranslit.to_latin(u"Диктерев",'ru')))
