import pandas as pd

def sec_to_format(s):
    h,s = divmod(int(s),3600)
    m,s = divmod(s,60)
    return f'{h:02}:{m:02}:{s:02}'

arquivo = 'exercicios.csv'

df = pd.read_csv(arquivo)

df['Duration'] = pd.to_timedelta(df['Duration'])
horas = df['Duration'].groupby(df['Type']).sum()

tabela1 = pd.pivot_table(df, index='Type', values=['Active Energy (kcal)', 'Distance (km)'], aggfunc='sum')
tabela1.at['Pool Swim', 'Distance (km)'] = tabela1.at['Pool Swim', 'Distance (km)'] / 1000
tabela2 = pd.pivot_table(df, index='Type', values=['Active Energy (kcal)'], aggfunc='count').rename(columns={'Active Energy (kcal)': 'Quantidade'})

tabela3 = pd.concat([tabela1, tabela2, horas], axis=1).reset_index()

tabela3 = tabela3.replace(['Outdoor Cycling', 'Pool Swim', 'Traditional Strength Training', 'Outdoor Walk'], ['Ciclismo', 'Natação', 'Musculação', 'Caminhada'])
tabela3 = tabela3.rename(columns={'Type': 'Modalidade', 'Active Energy (kcal)': 'Calorias', 'Distance (km)': 'Distância'})
tabela3['Calorias médias'] = tabela3['Calorias'] / tabela3['Quantidade']
tabela3['Distância média'] = tabela3['Distância'] / tabela3['Quantidade']
tabela3 = tabela3.round(2).sort_values(by=['Modalidade']).reset_index()
tabela3.loc['Total'] = tabela3.sum()
tabela3['Duração'] = [sec_to_format(s) for s in tabela3['Duration'].dt.total_seconds()]
tabela3.at['Total', 'Modalidade'] = ''
tabela3 = tabela3[['Modalidade', 'Quantidade', 'Duração', 'Calorias', 'Calorias médias', 'Distância', 'Distância média']]
tabela3.to_csv('Atividades Físicas.csv', index=False)
print(tabela3)