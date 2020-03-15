import os
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from sklearn.base import TransformerMixin



def add_column(dataframe, column_name):
    dataframe[column_name] = np.nan
    return dataframe


def ffill_na(dataframe, columns):
    dataframe.loc[:, columns].fillna(method='ffill', inplace=True)
    return dataframe


def convert_column_type(dataframe, column_name, to_type):
    dataframe[column_name] = dataframe[column_name].astype(to_type, False)
    return dataframe


def set_index(dataframe, column):
    dataframe = dataframe.set_index(column)
    return dataframe


def set_gender(dataframe, index_column_name):
    tumor_info = dataframe['Region'].groupby(index_column_name).first()
    female = tumor_info.str.endswith(u'ж')
    male = tumor_info.str.endswith(u'ч')
    dataframe['Gender'] = np.nan
    dataframe.loc[female, 'Gender'] = 'F'
    dataframe.loc[male, 'Gender'] = 'M'
    return dataframe


def set_tumor_code(dataframe, index_column_name):
    tumor_info = dataframe['Region'].groupby(index_column_name).first()
    tumor_code = tumor_info.str.extractall(r'([СC]\d+\.?\d?)').groupby('Group').transform(lambda x: ', '.join(x))
    tumor_code.index = tumor_code.index.droplevel(1)
    tumor_code = tumor_code[~tumor_code.index.duplicated(keep='last')]
    tumor_code.columns = ['Tumor code']
    dataframe = dataframe.merge(tumor_code, how='left', left_index=True, right_index=True)
    return dataframe


def set_tumor_group(dataframe, index_column_name):
    tumor_info = dataframe['Region'].groupby(index_column_name).first()
    dataframe = dataframe.merge(tumor_info, how='left', left_index=True, right_index=True, suffixes=('', '_tg'))
    dataframe.rename(
        columns={'Region_tg': 'Tumor group'},
        inplace=True
    )
    dataframe['Tumor group'] = dataframe['Tumor group'].str[:-1].str.strip()
    dataframe = dataframe.reset_index()
    dataframe = dataframe.set_index('Tumor code')
    tumor_group = dataframe['Tumor group'].groupby('Tumor code').first()
    dataframe = dataframe.join(tumor_group, lsuffix='_drop')
    dataframe = dataframe.drop('Tumor group_drop', axis=1)
    dataframe = dataframe.reset_index()
    dataframe = dataframe.set_index(index_column_name)
    return dataframe


def rename_columns(dataframe, columns):
    dataframe.rename(columns=columns, inplace=True)
    return dataframe


def add_year(dataframe, year):
    dataframe['Year'] = year
    return dataframe


def get_pipeline_steps(skip_rows, year):
    steps = [
        ('Read datafarme from excel',
         FunctionTransformer(lambda data_path: pd.read_excel(data_path, skiprows=range(skip_rows)))),
        ('Rename columns',
         FunctionTransformer(lambda x: rename_columns(x, {
             'Unnamed: 0': 'Group',
             'Unnamed: 1': 'Region',
             'Unnamed: 2': 'Count'
         }))
         ),
        ('Fill na in the Group column',
         FunctionTransformer(lambda x: ffill_na(x, 'Group'))
         ),
        ('Drop first row',
         FunctionTransformer(lambda x: x.drop(0))),
        ('Convert group column to int',
         FunctionTransformer(lambda x: convert_column_type(x, 'Group', np.int))),
        ('Set index to Group column',
         FunctionTransformer(lambda x: set_index(x, 'Group'))),
        ('Set gender',
         FunctionTransformer(lambda x: set_gender(x, 'Group'))),
        ('Set tumor code',
         FunctionTransformer(lambda x: set_tumor_code(x, 'Group'))),
        ('Set tumor group',
         FunctionTransformer(lambda x: set_tumor_group(x, 'Group'))),
        ('Drop first row in each group',
         FunctionTransformer(lambda x: x.groupby('Group').apply(lambda group: group.iloc[1:, :]).droplevel(1))),
        ('Add year',
         FunctionTransformer(lambda x: add_year(x, year)))
    ]
    return steps


def main(data_path):
    pipeline = Pipeline(steps=get_pipeline_steps(7, 2018))

    df = pipeline.transform(data_path)
    x = 1
    pas7


if __name__ == '__main__':
    data_path = os.path.join('data', 'cancer-2019.xls')
    main(data_path)