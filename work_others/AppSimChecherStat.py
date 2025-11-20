import asyncio
from SSHClientClass import AsyncSSHClient
from ClassConverter import DataConverter
import logging
import json


#print("Текущая папка:", os.getcwd())

#print(dir(DataConverter))
# print("Пути поиска Python:")
# for path in sys.path:
#     print(" -", path)
# создаем класс конвертер




async def AppSimCheckerStat(date: str=None):
    '''по ssh получаем json с сервера, обрабатываем результаты, собираем статистику и экспортируем в excel'''
    ssh_client = AsyncSSHClient(host='dialer-crm.rs.ru', username='beginin-ov' , password='RKqxFwZWZTgGUmMXtrbX')
    # получаем и парсим данные с сервера из json
    data = await ssh_client.request_appSimChecker(date = date)
    #print(data)

    # получаем и парсим данные с файла json
    # file_path = os.path.abspath(r"C:\Users\beginin-ov\Projects\Local\Converter\files/2025-10-24.json")
    # file_path = r"C:\Users\beginin-ov\Projects\Local\Converter\files\2025-10-24.json"
    # print(file_path)
    # converter = DataConverter()
    # data = converter.json_to_python(input_file=file_path)

    #print(f"data {data}")
    # for line in data:
    #     print(line)

    # Проверка на уникальность callerid
    #callerid_ext = [record.get('callerid_ext') for record in data]
    #callerid_ext = Counter(callerid_ext)
    # print(callerid_ext.most_common())
    # print("По убыванию:", dict(callerid_ext.most_common()))

    #callerid = [record.get('callerid') for record in data]
    #callerid = Counter(callerid)
    #print(callerid.most_common())
    #print("По убыванию:", dict(callerid.most_common()))


    # объединяем данные для одинаковых callerid
    new_dict = {}
    #print(counter_dict)
    for record in data:
        callerid = record.get("callerid")
        if callerid in new_dict:
            new_dict[callerid]["attempts"] += 1
            if record.get("res") ==1:
                new_dict[callerid]["successful"] +=1
            else:
                new_dict[callerid]["failed"] += 1

            new_dict[callerid]["dates"].append(record.get("date"))
            new_dict[callerid]["operator"].add(record.get("operator"))
            new_dict[callerid]["callerid_ext"].add(record.get("callerid_ext"))
        else:
            new_dict[callerid] = {
                "attempts": 1,
                'successful': 1 if record.get("res") == 1 else 0,
                "failed": 0 if record.get("res") == 1 else 1,
                'dates': [record.get("date")],
                'operator': {record.get("operator")},
                'callerid_ext': {record.get("callerid_ext")},
            }
    #print(new_dict)
    # конвертер в результатов в excel
    converter = DataConverter()
    converter.python_to_excel(data=new_dict, output_file= f'AppSimChecker_stat_{date}.xlsx', key_name='caller_id')

    # вывод результатов: ключ -значение
    # for key, stat in new_dict.items():
    #     print(f"{key}: {stat}")
    print(f"Лог за дату: {date}")

    # выделям операторов
    operators_dict = {operator: {} for operator in sorted(set(
        record.get('operator') for record in data
        if record.get('operator')
    ))}
    #print(operators_dict)

    # подсчет количество caller_id по операторам
    for operator in operators_dict.keys():
        stats =await count_callerid(operator=operator, data=new_dict)
        operators_dict[operator] = stats[operator]
    #print(operators_dict)
    print(json.dumps(operators_dict, ensure_ascii=False, indent=2))

    return operators_dict


async def count_callerid(operator: str, data: dict):
    '''фукция для подсчет количества включенных/выключенных callerid'''
    on = 0
    off = 0
    total = 0
    for record in data.values():
        if 'operator' in record and operator in record['operator']:
            total +=1
            if record['successful'] > 0:
                on +=1
            else:
                off +=1
    return {f"{operator}": {"Всего": total, "Включенные": on,"Выключенные": off}}


async def main():
    logging.basicConfig(level=logging.INFO)
    await AppSimCheckerStat(date='2025-10-30')
asyncio.run( main())
