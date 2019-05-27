#!/python

from datetime import datetime
from io import BytesIO
import xml.etree.cElementTree as ET
from xclasses import *

def send_1s(filename):
    cfg = Config('msg_1c.json')
    root = ET.Element("ФайлОбмена",ВерсияФормата="3.1",ДатаВыгрузки=datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),ИмяКонфигурацииИсточника="МикрофинансоваяОрганизация",ИмяКонфигурацииПриемника="БухгалтерияДляКазахстанаВцИнтеграцияМФО",ИдПравилКонвертации="ea06f991-26be-42ae-98b6-cad3112dc97d")
    rules = ET.parse('send_1s_rules.xml')
    xdate = rules.find('ДатаВремяСоздания')
    xdate.text = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    root.append(rules.getroot())
    types = ET.parse('send_1s_types.xml')
    root.append(types.getroot())
    cfg.data['НомерИсходящегоСообщения'] += 1
    m_out = str(cfg.data['НомерИсходящегоСообщения'])
    m_in = str(cfg.data['НомерВходящегоСообщения'])
    ET.SubElement(root,"ДанныеПоОбмену",ПланОбмена="ОбменМикрофинансоваяОрганизацияБухгалтерия30",Кому=cfg.data['Кому'],ОтКого=cfg.data['ОтКого'],НомерИсходящегоСообщения=m_out,НомерВходящегоСообщения=m_in,УдалитьРегистрациюИзменений="true").text = ''

    tree = ET.ElementTree(root)
    tree.write(filename,encoding="UTF-8",xml_declaration=True,short_empty_elements=True)
    cfg.save()

def main():
    send_1s("filename.xml")

if __name__ == '__main__':
    main()
