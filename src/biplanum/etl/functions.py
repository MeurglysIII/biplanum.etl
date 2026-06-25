"""
ETL утилиты для работы с кубами Planum OLAP и DataFrame pandas.
Модуль предоставляет набор функций для извлечения, трансформации и загрузки данных
из/в кубы Planum OLAP. 
Включает утилиты для:
- Экспорта данных куба в DataFrame pandas
- Загрузки DataFrame обратно в кубы
- Очистки данных куба с по области
- Валидации и трансформации данных
- Логирования и отладки

Классы:
    LogWrite: Перенаправляет вывод print в логгер Planum.

Функции:
    createConnection: Создание соединения с моделью Планума
    CellExportPy: Экспорт данных куба в виде DataFrame.
    CellExportPy_areaList: Экспорт данных из нескольких областей куба.
    loadDataframeInCube: Загрузка значений DataFrame в куб.
    clearCubePy: Очистка полного куба или определённой области.
    clearCubePy_areaList: Очистка нескольких областей куба.
    removeRowsWithNonexistElem: Фильтрация строк DataFrame с несуществующими элементами измерения.
    pivotDataframe: Трансформация DataFrame для преобразования показателей в колонки.
    printDataframe: Красивый вывод содержимого DataFrame в лог.

Константы модуля:
    outputWidth: Ширина форматированного вывода в консоль (по умолчанию: 120).
    fillSymbol: Символ для форматирования границ вывода (по умолчанию: '#').

Зависимости:
- pandas
- numpy
- Planum.DAL (Database, Cube)
"""
import pandas as pd
from typing import List, Dict
from Planum.DAL import Database, Cube
from Planum.DAL.Model import CellType, CellArea
from Planum.Process.Connections import PlanumConnection
from collections.abc import Generator, Iterable
import numpy as np

outputWidth = 120
fillSymbol = '#'
valueName = 'Value'

class LogWrite:
    """
    Класс для перенаправления вывода print в лог Planum. Обычный print работает неправильно.     
    """
    def __init__(self, LOG):
        self.LOG = LOG
    def getvalue(self):
        return None
    def write(self, string):
        if string != '\n':
            for line in string.split('\n'):
                if line: self.LOG.Info(line)

class CubePy(Cube):
    name:str
    cubeObj:Cube
    dimNameMeasure:str

    def __init__(self, cubeName:str, database:Database):
        super().__init__(database, cubeName=cubeName) 
        self.name = cubeName
        self.dimNameMeasure = self.CubeDimensions()[-1].Info().ndimension

def createConnection(Host:str, Port:str, Database:str, User:str, Password:str, 
    UseSsl:bool = False)->PlanumConnection:
    """Создаёт соединение с моделью"""
    # Создаём пустое соединение
    obj = PlanumConnection() 
    
    # Для каждого параметра вызываем метод setter
    for key, value in locals().items():
        setter_name = f"set_{key}"
        if hasattr(obj, setter_name): # Если есть метод setter
            getattr(obj, setter_name)(value)
        # Если у класса есть параметр в конструкторе
        elif hasattr(obj, key):
            setattr(obj, key, value)
            
    return obj

# Вывод dataframe в логи
def printDataframe(df:pd.DataFrame, max_rows=6, df_name:str=None):
    """
    Выводит в лог содержимое dataframe. Более читаемо, чем просто print(df). 
    """
    nrows = len(df)
    print(f"Cтолбцы dataframe{f"' {df_name}'" if df_name else ''}: {df.columns.tolist()}")
    if nrows <= max_rows or max_rows == 1: # Если все строки умещаются в max_rows
        for i, row in enumerate(df.values):
            print(f"'{df.iloc[i].name}': {str(row.tolist())}")
    else: # Если всего строк больше, чем max_rows
        # Выводим первые и последние строки
        for i in range(max_rows//2):
            print(f"'{df.iloc[i].name}': {str(df.iloc[i].tolist())}")
        print('.'*10)
        for i in range(nrows - max_rows + max_rows//2, nrows):
            print(f"'{df.iloc[i].name}': {str(df.iloc[i].tolist())}")

def CellExportPy (cube:Cube, area:Dict[str,List[str] | str]|None = None, use_rules=True, base_only=True, skip_empty=True, 
                  show_rule=True, verbose=True, silent=False, blocksize:int|None = None) -> pd.DataFrame|Generator[pd.DataFrame, None, None]:
    """
    Загрузка данных из куба с помощью метода CellExport. Позволяет выгрузить срез куба в виде dataframe с измерениями в столбцах и значениями в столбце Value.

    :param area: словарь вида {dimName: elementName} или {dimName: [elementName1, elementName2,...]} для указания среза.
    :param use_rules: грузить ячейки, к которым применяются правила
    :param base_only: грузить только базовые элементы
    :param skip_empty: пропускать пустые ячейки
    :param show_rule: непонятно, что делает. Используйте use_rules
    :param verbose: выводить ли информацию о размере и столбцах полученного dataframe
    :param silent: отключить все выводы, включая информацию о размере и столбцах полученного dataframe. Параметр полезен при загрузке нескольких областей, чтобы не засорять лог повторяющейся информацией. 
    :param blocksize: размер блока. Если указан, то функция возвращает генератор, который отдёт Dataframe по блокам указанного размера
    """

    cubeName = cube.CurrentInfo.name_cube
    cube_dims = cube.CubeDimensions()
    
    
    # Обработка параметра среза
    if area is None:
        area_int = None
    else:
        area_int = []
        for i, dim in enumerate(cube_dims):
            dimName = dim.Info().ndimension
            if dimName in area:
                if isinstance(area[dimName], str): # если только один элемент в области
                    try: # На случай, если элемента нет в измерении
                        el_id = dim.Element(elementName=area[dimName]).Info().element
                    except Exception as e:
                        print(f'Элемент {element_str} не найден в измернии {dimName}')
                        raise e
                    area_int.append([el_id])
                else: # если несколько элементов в области
                    this_area = []
                    for element_str in area[dimName]:
                        try: # На случай, если элемента нет в измерении
                            el_id = dim.Element(elementName=element_str).Info().element
                        except Exception as e:
                            print(f'Элемент {element_str} не найден в измернии {dimName}')
                            raise e
                        this_area.append(el_id)
                    area_int.append(this_area)
            elif base_only: # Если срез не задан и есть флаг на базовые элементы
                area_int.append([el.element for el in dim.ElementInfos() if el.level == 0])
            else: # Если срез не задан
                area_int.append([])
        base_only = False # т.к. выше уже обработали по этому флагу
    
    #Извлечение среза
    if not silent:
        print(f" Начало загрузки данных из куба {cubeName} ".center(outputWidth, fillSymbol))
    
    def cellAreasToDataframe(cellAreas:Iterable[CellArea])->pd.DataFrame:
        # Преобразование в dataframe (сначала в словари)
        dimNames = [dim.Info().ndimension for dim in cube_dims]
        elNames = [{el.element: el.element_name for el in dim.ElementInfos()} for dim in cube_dims]
        list_areas = [[elNames[i][element_id] for i, element_id in enumerate(cellArea.path)] 
                    + [float(cellArea.value) if cellArea.type == CellType.Numeric else cellArea.value] 
                    for cellArea in cellAreas ]

        return pd.DataFrame(list_areas, columns= dimNames + [valueName]) 

    if blocksize:
        def generator_df()->Generator[pd.DataFrame, None, None]:
            """
            Генератор Dataframe по блокам
            """
            lastPath = None # Хранит последний путь, с которого будет грузиться следующий блок
            counter = 0 # Номер блока
            while(True):
                cellAreas = cube.CellExport(area=area_int, use_rules=use_rules, base_only=base_only, skip_empty=skip_empty, 
                                            show_rule=show_rule, blocksize=blocksize, path=lastPath)
                len_areas = len(list(cellAreas))
                if not len_areas: #Если пустой блок
                    break

                lastPath = list(cellAreas)[-1].path # Последний путь, с которого будет грузиться следующий блок

                df = cellAreasToDataframe(cellAreas)

                isLastBlock = len_areas < blocksize

                if not silent:
                    if verbose:
                        print(f"Размерность полученного df: {df.shape}")
                        print(f"Полученные столбцы: {df.columns.tolist()}")
                        print(f"Первая строка: {str(df.head(1).values.tolist())}")
                        print(f"Последняя строка: {str(df.tail(1).values.tolist())}")
                    print(f" Данные из куба '{cubeName}' успешно загружены ({len(df)} ячеек, блок №{counter})".center(outputWidth, fillSymbol)) 
                    print(f" Все блоки данных из куба '{cubeName}' успешно загружены ({blocksize*counter + len_areas} ячеек, {counter+1} блоков)".center(outputWidth, fillSymbol)) 

                yield df

                if isLastBlock:
                    break
                counter += 1
        
        return generator_df()
    else: # Загрузка одним блоком
        cellAreas = cube.CellExport(area=area_int, use_rules=use_rules, base_only=base_only, skip_empty=skip_empty, 
                                    show_rule=show_rule, blocksize=1_000_000_000, path=None)

        df = cellAreasToDataframe(cellAreas)
        
        if not silent:
            if verbose:
                print(f"Размерность полученного df: {df.shape}")
                print(f"Полученные столбцы: {df.columns.tolist()}")
                print(f"Первая строка: {str(df.head(1).values.tolist())}")
                print(f"Последняя строка: {str(df.tail(1).values.tolist())}")
            print(f" Данные из куба '{cubeName}' успешно загружены ({len(df)} ячеек) ".center(outputWidth, fillSymbol)) 

        return df

def CellExportPy_areaList (cube:Cube, areas:List[Dict[str,List[str] | str]], use_rules=True, base_only=True, skip_empty=True, 
                  show_rule=True, verbose=True, silent=False):
    """
    Выгрузка данных из куба для списка областей. Для каждой области вызывается функция CellExportPy, а полученные dataframes объединяются в один. 

    :param areas: список словарей вида {dimName: elementName} или {dimName: [elementName1, elementName2,...]} для указания среза.
    :param use_rules: грузить ячейки, к которым применяются правила
    :param base_only: грузить только базовые элементы
    :param skip_empty: пропускать пустые ячейки
    :param show_rule: непонятно, что делает. Используйте use_rules
    :param verbose: выводить ли информацию о размере и столбцах полученного dataframe
    :param silent: отключить все выводы, включая информацию о размере и столбцах полученного dataframe. Параметр полезен при загрузке нескольких областей, чтобы не засорять лог повторяющейся информацией. 
    """
    cubeName = cube.CurrentInfo.name_cube
    if not silent:
        print(f"Начало загрузки данных из куба {cubeName}".center(outputWidth, fillSymbol))
    
    df_list = []
    for area in areas: # Каждую область загружаем отдельно и потом объединяем
        df_i = CellExportPy(cube, area, use_rules, base_only, skip_empty, show_rule, verbose, silent=True)
        if not df_i.empty:
            df_list.append(df_i)
    # Объединяем все dataframes в один
    if not df_list:
        df = pd.DataFrame()
    else:
        df = pd.concat(df_list, ignore_index=True)
    
    if not silent:
        if verbose:
            print(f"Размерность полученного df: {df.shape}")
            print(f"Полученные столбцы: {df.columns.tolist()}")
            print(f"Первая строка: {str(df.head(1).values.tolist())}")
            print(f"Последняя строка: {str(df.tail(1).values.tolist())}")
        print(f"Данные из куба '{cubeName}' успешно загружены ({len(df)} ячеек)".center(outputWidth, fillSymbol))
    return df

def loadDataframeInCube(df:pd.DataFrame, cube:Cube, add:bool=False):
    """
    Загрузка данных из dataframe в куб. Dataframe должен содержать столбец Value со значениями и столбцы с именами измерений, совпадающими с именами измерений в кубе.

    :param df: dataframe для загрузки. Должен содержать столбец Value со значениями и столбцы с именами измерений, совпадающими с именами измерений в кубе.
    :param cube: куб для загрузки данных
    :param add: флаг, указывающий, нужно ли добавлять значения к существующим в кубе (True) или перезаписывать их (False).
    """
    cubeName = cube.CurrentInfo.name_cube

    if df.empty:
        print(f"Попытка загрузить пустой dataframe в куб {cubeName}")
        return

    df = df.copy() # Копия, чтобы не менять оригинал
    # Заменяем символы, которые вызывают ошибки при записи (на новых версиях Планума это не нужно)
    #df[valueName] = df[valueName].apply(lambda x: x.replace(':','։') if isinstance(x, str) else x)
    #df[valueName] = df[valueName].apply(lambda x: x.replace('"',"'") if isinstance(x, str) else x)

    # Оставляем только измерения куба и Value в столбцах
    cube_dims = cube.CubeDimensions()
    dimNames = [dim.Info().ndimension for dim in cube_dims]
    columns_to_keep = dimNames+[valueName]
    all_columns = list(df.columns)
    # Проверяем, что все измерения есть среди столбцов
    for column in columns_to_keep:
        if not column in all_columns:
            raise ValueError(f'Не найдено измерение "{column}" среди столбцов dataframe куба "{cubeName}"')
    df = df[dimNames+[valueName]]

    # Консолидируем, если нужно
    if add:
        df = df.groupby([col for col in df.columns if col != 'Value']).sum().reset_index()

    # формируем список со значениями 
    cube_values = df[valueName].tolist()

    # формируем список координат

    elIds = {dim.Info().ndimension : {el.element_name: el.element for el in dim.ElementInfos()} for dim in cube_dims}
    # Добавляем алиасы к мэппингу элемент-координата
    for dim in cube_dims:
        dimName = dim.Info().ndimension

        aliasNames = [el.element_name for el in dim.AttributeDimension().ElementInfos()]
        aliasNames = [a for a in aliasNames if a[0] == "@"] # алиасы начинаются с @

        if aliasNames:
            # Грузим алиасы из куба атрибутов
            df_attr = CellExportPy (dim.AttributeCube(), {
                dim.AttributeDimension().CurrentInfo.ndimension:aliasNames}, 
                silent=True)

            elIds_for_alias = dict(zip(df_attr[valueName], df_attr[dimName].map(elIds[dimName])))

            elIds[dimName].update(elIds_for_alias)

    # Проверяем существование элементов измерений
    for dim in cube_dims:
        dimName = dim.Info().ndimension
        elNames = set(elIds[dimName].keys())
        df_isin = df[dimName].isin(elNames)
        if not df_isin.all(): # Если есть несуществующие элементы
            df_first_line = df[~df_isin].head(1)
            element_nonexist = df_first_line[dimName].to_list()[0]
            print(f'Найден несуществующий элемент "{element_nonexist}" измерения "{dimName}" в следующей строчке:')
            printDataframe(df_first_line)
            raise ValueError(f'Найден несуществующий элемент "{element_nonexist}" измерения "{dimName}"')

    # Преобразуем текстовые названия элементов в их id
    for dimName in dimNames:
        df[dimName] = df[dimName].map(elIds[dimName])

    coordinates = df[dimNames].values.tolist()

    # загрузка в куб-приемник
    print(f" Начало загрузки в куб '{cubeName}' ({len(df)} ячеек) ".center(outputWidth, fillSymbol))
    
    # Загрузка кусочками
    nRows = len(cube_values)
    chunk_size = 100_000
    chunk_start = 1
    while chunk_start <= nRows:
        chunk_end = min (chunk_start + chunk_size - 1, nRows)
        cube.SetValuesBulk(cube_values[chunk_start-1:chunk_end], coordinates[chunk_start-1:chunk_end])
        chunk_start += chunk_size

    # cube.SetValuesBulk(values=cube_values, coords=coordinates, add=add)
    print(f" Загрузка в куб '{cubeName}' завершена ".center(outputWidth, fillSymbol))


def clearCubePy(cube:Cube, area:Dict[str,List[str] | str] | None = None, silent=False):
    """
    Очистка среза куба. Если параметр area не задан, очищает весь куб. Если задан, очищает только указанный срез.

    :param cube: куб для очистки
    :param area: словарь вида {dimName: elementName} или {dimName: [elementName1, elementName2,...]} для указания среза.
    :param silent: флаг, указывающий, нужно ли выводить сообщения о ходе выполнения
    """
    cube_dims = cube.CubeDimensions()
    cubeName = cube.CurrentInfo.name_cube
    
    # Обработка параметра среза
    if area is None:
        #Очистка
        cube.ClearComplete()
    else:
        area_int = []
        for i, dim in enumerate(cube_dims):
            dimName = dim.Info().ndimension
            if dimName in area:
                if isinstance(area[dimName], str): # если только один элемент в области
                    area_int.append([dim.Element(elementName=area[dimName]).Info().element])
                else:                    # если несколько элементов в области
                    area_int.append([dim.Element(elementName=element_str).Info().element for element_str in area[dimName]])
            else: # Если срез не задан
                area_int.append([])
        #Очистка
        cube.ClearArea(area_int)
    if not silent:
        print(f" Срез куба '{cubeName}' очищен ".center(outputWidth, fillSymbol))

def clearCubePy_areaList(cube:Cube, areas:List[Dict[str,List[str] | str]], silent=False):
    """
    Очистка среза куба для списка областей. Для каждой области вызывается функция clearCubePy.

    :param areas: список словарей вида {dimName: elementName} или {dimName: [elementName1, elementName2,...]} для указания среза.
    :param cube: куб для очистки
    :param silent: флаг, указывающий, нужно ли выводить сообщения о ходе выполнения
    """
    cubeName = cube.CurrentInfo.name_cube
    
    #Очистка
    for area in areas:
        clearCubePy(cube, area, silent=True)

    if not silent:
        print(f"Срез куба '{cubeName}' очищен".center(outputWidth, fillSymbol))


def removeRowsWithNonexistElem(df:pd.DataFrame, dimName: str, database: Database):
    """
    Очистка dataframe от строк, где есть несуществующие элементы измерения.

    :param df: dataframe для очистки
    :param dimName: имя измерения
    :param database: база данных
    """
    df = df.copy()
    # Убираем лишние пробелы с краёв
    df[dimName] = df[dimName].apply(lambda x: x.strip() if isinstance(x, str) else str(x))

    dimension = database.Dimension(dimName)
    aliasNames = [el.element_name for el in dimension.AttributeDimension().ElementInfos()]
    aliasNames = [a for a in aliasNames if a[0] == "@"] # алиасы начинаются с @

    # Имена элементов основные
    elNames = [el.element_name for el in dimension.ElementInfos()]
    # Грузим алиасы из куба атрибутов
    df_attr = CellExportPy (dimension.AttributeCube(), verbose=False)
    # Добавляем алиасы в общий список элементов
    for alias in aliasNames:
        elNames += df_attr[df_attr.iloc[:, 0] == alias][valueName].tolist()
    # Проверям, есть ли элемент в измерении
    boolArrayElExists = df[dimName].isin(elNames)
    foundNonexistElements = (~boolArrayElExists).values.any()
    if foundNonexistElements:
        print(f"Найдены несуществующие элементы измерения '{dimName}' в dataframe: {df[~boolArrayElExists][dimName].unique()}")
    else:
        print(f"Все элементы столбца '{dimName}' существуют в измерении")
    
    return df[boolArrayElExists]

def pivotDataframe(df:pd.DataFrame, dimNameMeasure: str) -> pd.DataFrame:
    """
    Переводит показатели в столбцы.

    :param df: dataframe для перевода
    :param dimNameMeasure: имя измерения показателей
    :return: dataframe с показателями в столбцах. Столбцы с именами измерений, кроме dimNameMeasure, сохраняются. Столбец Value с показателями удаляется, а показатели становятся значениями в новых столбцах.
    """
    index_columns = [col for col in df.columns if col != dimNameMeasure and col != valueName]
    df_pivoted = df.pivot(index=index_columns,columns=dimNameMeasure, values = valueName)
    df_pivoted.replace({np.nan: None}, inplace=True)
    df_pivoted.reset_index(inplace=True) # Возвращаем остальные колонки из индекса
    
    return df_pivoted