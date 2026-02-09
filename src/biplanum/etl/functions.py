import pandas as pd
from typing import List, Dict
from Planum.DAL import Database, Server, IOlapConnection, Dimension, Cube
from Planum.DAL.Model import ElementType, CellType
import time, sys
import numpy as np
from Planum.Process import ProjectContextLogger


outputWidth = 120
fillSymbol = '#'

class LogWrite:
    def __init__(self, LOG):
        self.LOG = LOG
    def getvalue(self):
        return None
    def write(self, string):
        getvalue = self
        if string != '\n':
            for line in string.split('\n'):
                self.LOG.Info(line)

# Загрузка среза из куба
def CellExportPy (cube: Cube, area: Dict[str,List[str]] = None, use_rules=True, base_only=True, skip_empty=True, 
                  show_rule=True, verbose=True, silent=False):
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
                if area[dimName] is str: # если только один элемент в области
                    area_int.append([dim.Element(elementName=area[dimName]).Info().element])
                else:                    # если несколько элементов в области
                    area_int.append([dim.Element(elementName=element_str).Info().element for element_str in area[dimName]])
            elif base_only: # Если срез не задан и есть флаг на базовые элементы
                area_int.append([el.element for el in dim.ElementInfos() if el.level == 0])
            else: # Если срез не задан
                area_int.append([])
        base_only = False # т.к. выше уже обработали по этому флагу
    
    #Извлечение среза
    if not silent:
        print(f" Начало загрузки данных из куба {cubeName} ".center(outputWidth, fillSymbol))
    cellAreas = cube.CellExport(area=area_int, use_rules=use_rules, base_only=base_only, skip_empty=skip_empty, 
                                show_rule=show_rule, blocksize=1_000_000_000)

    # Преобразование в dataframe (сначала в словари)
    dimNames = [dim.Info().ndimension for dim in cube_dims]
    elNames = [{el.element: el.element_name for el in dim.ElementInfos()} for dim in cube_dims]
    data_dict = []
    for cellArea in cellAreas:
        area_dict = {}
        for i, element_id in enumerate(cellArea.path):
            area_dict[dimNames[i]] = elNames[i][element_id]
        cellType = cellArea.type
        if (cellType == CellType.Numeric):
            value = float(cellArea.value)
        else:
            value = cellArea.value
        area_dict["Value"] = value
        data_dict.append(area_dict)

    df = pd.DataFrame(data_dict) 
    
    if not silent:
        if verbose:
            print(f"Размерность полученного df: {df.shape}")
            print(f"Полученные столбцы: {df.columns.tolist()}")
            print(f"Первая строка: {str(df.head(1).values.tolist())}")
            print(f"Последняя строка: {str(df.tail(1).values.tolist())}")
        print(f" Данные из куба '{cubeName}' успешно загружены ({len(df)} ячеек) ".center(outputWidth, fillSymbol)) 
    return df

# Выгрузка данных из куба для списка областей
def CellExportPy_areaList (cube: Cube, areas: List[Dict[str,List[str]]] = None, use_rules=True, base_only=True, skip_empty=True, 
                  show_rule=True, verbose=True, silent=False):
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

# Выгрузка dataframe в куб
def loadDataframeInCube(df: pd.DataFrame, cube: Cube, add:bool=False):
    cubeName = cube.CurrentInfo.name_cube

    if df.empty:
        print(f"Попытка загрузить пустой dataframe в куб {cubeName}")
        return

    df = df.copy() # Копия, чтобы не менять оригинал
    # Заменяем символы, которые вызывают ошибки при записи
    df["Value"] = df["Value"].apply(lambda x: x.replace(':','։') if isinstance(x, str) else x)
    df["Value"] = df["Value"].apply(lambda x: x.replace('"',"'") if isinstance(x, str) else x)

    # формируем список со значениями 
    cube_values = df['Value'].tolist()

    # формируем список координат
    coordinates = df[[dim.Info().ndimension for dim in cube.CubeDimensions()]].values.tolist()

    # загрузка в куб-приемник
    print(f" Начало загрузки в куб '{cubeName}' ({len(df)} ячеек) ".center(outputWidth, fillSymbol))
    
    cube.SetValuesBulk(values=cube_values, coords=coordinates, add=add)
    print(f" Загрузка в куб '{cubeName}' завершена ".center(outputWidth, fillSymbol))

# Очистка среза
def clearCubePy(cube: Cube, area: Dict[str,List[str]] = None, silent=False):
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
                if area[dimName] is str: # если только один элемент в области
                    area_int.append([dim.Element(elementName=area[dimName]).Info().element])
                else:                    # если несколько элементов в области
                    area_int.append([dim.Element(elementName=element_str).Info().element for element_str in area[dimName]])
            else: # Если срез не задан
                area_int.append([])
        #Очистка
        cube.ClearArea(area_int)
    if not silent:
        print(f" Срез куба '{cubeName}' очищен ".center(outputWidth, fillSymbol))

# Очистка среза для списка областей
def clearCubePy_areaList(cube: Cube, areas: List[Dict[str,List[str]]] = None, silent=False):
    cubeName = cube.CurrentInfo.name_cube
    
    #Очистка
    for area in areas:
        clearCubePy(cube, area, silent=True)

    if not silent:
        print(f"Срез куба '{cubeName}' очищен".center(outputWidth, fillSymbol))

# Очистка dataframe от строк, где есть несуществующие элементы измерения
def removeRowsWithNonexistElem(df, dimName: str, database: Database):
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
        elNames += df_attr[df_attr.iloc[:, 0] == alias]["Value"].tolist()
    # Проверям, есть ли элемент в измерении
    boolArrayElExists = df[dimName].isin(elNames)
    foundNonexistElements = (~boolArrayElExists).values.any()
    if foundNonexistElements:
        print(f"Найдены несуществующие элементы измерения '{dimName}' в dataframe: {df[~boolArrayElExists][dimName].unique()}")
    else:
        print(f"Все элементы столбца '{dimName}' существуют в измерении")
    
    return df[boolArrayElExists]

# Переводи показателей в столбцы
def pivotDataframe(df: pd.DataFrame, dimNameMeasure: str) -> pd.DataFrame:
    valueName = "Value"
    index_columns = [col for col in df.columns if col != dimNameMeasure and col != valueName]
    df_pivoted = df.pivot(index=index_columns,columns=dimNameMeasure, values = valueName)
    df_pivoted.replace({np.nan: None}, inplace=True)
    df_pivoted.reset_index(inplace=True) # Возвращаем остальные колонки из индекса
    
    return df_pivoted

# Вывод dataframe в логи
def printDataframe(df: pd.DataFrame, max_rows=None, df_name=None):
    print(f"Cтолбцы dataframe{f"' {df_name}'" if df_name else ''}: {df.columns.tolist()}")
    for i, row in enumerate(df.values):
        if max_rows and i > max_rows: break
        print(f"'{df.iloc[i].name}': {str(row.tolist())}")
