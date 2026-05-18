from variable_utils import *

from variable_utils import *
import os

BASE_DIR = Path.cwd()

MOCK_DATA_PATH = BASE_DIR/f'mock_data/MOCK_DATA.csv'

df = pd.read_csv(MOCK_DATA_PATH)

# dividindo em dez partes o meu mock_data
NUM_PARTES = 10
TAMANHO_PARTE = len(df) // NUM_PARTES

data_iso = list(
    reversed(
        [(datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(NUM_PARTES)]
    )
)

os.makedirs(f'{DATALAKE_PATH}/source/transaction_system', exist_ok=True)

for i, data_str in enumerate(data_iso):
    print(f"Salvando arquivo: 'transaction_data_{data_str.replace('-', '_')}.csv'")
    inicio = i * TAMANHO_PARTE
    fim = (i + 1) * TAMANHO_PARTE if i < NUM_PARTES - 1 else len(df)
    df_parte = df.iloc[inicio:fim].copy()
    df_parte['transaction_date'] = data_str
    df_parte.to_csv(f"{DATALAKE_PATH}/source/transaction_system/transaction_data_{data_str.replace('-', '_')}.csv", index=False)