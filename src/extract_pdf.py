"""
============================================================================
Extração de Dados - PBEV (Programa Brasileiro de Etiquetagem Veicular)
Fonte: INMETRO / CONPET
============================================================================
Script genérico: funciona com PBEV 2024, 2025 ou qualquer edição futura
que mantenha a mesma estrutura de 28 colunas.

USO:
    python extract_pbev.py                          # usa PDF padrão
    python extract_pbev.py meu_arquivo.pdf          # usa PDF informado
============================================================================
"""

import pdfplumber
import pandas as pd
import numpy as np
import re
import sys
import os
import warnings
warnings.filterwarnings('ignore')


# ============================================================================
# CONFIGURAÇÃO - altere o caminho do PDF aqui ou passe por argumento
# ============================================================================
PDF_PADRAO = "../data/raw/pbe-veicular-2024-1.pdf"


# ============================================================================
# MAPEAMENTO DAS 28 COLUNAS DO PDF
# Nomes auto-explicativos com hierarquia dos cabeçalhos incorporada.
# Cada nome segue o padrão: [Grupo] [Subgrupo] [Detalhe] ([Unidade])
# ============================================================================
COLUNAS_PDF = {
    0:  "Categoria",
    1:  "Marca",
    2:  "Modelo",
    3:  "Versão",
    4:  "Motor",
    5:  "Tipo de Propulsão (Combustão / Híbrido / Plug-in / Elétrico)",
    6:  "Transmissão e Velocidades (Manual=M / Automática=A / Dupla Embreagem=DCT / Automatizada=MTA / Contínua=CVT)",
    7:  "Ar Condicionado (S=Sim / N=Não)",
    8:  "Direção Assistida (H=Hidráulica / M=Mecânica / E=Elétrica / E-H=Eletro-hidráulica)",
    9:  "Combustível (E=Elétrico / G=Gasolina / F=Flex / D=Diesel)",
    10: "Emissões Poluentes - NMOG+NOx (mg/km)",
    11: "Emissões Poluentes - CO (mg/km)",
    12: "Emissões Poluentes - CHO Aldeídos (mg/km)",
    13: "Emissões Poluentes - Redução Relativa ao Limite (A=≥40% abaixo PROCONVE L7 / B=<40%)",
    14: "Emissões GEE - CO2 Fóssil Etanol (g/km)",
    15: "Emissões GEE - CO2 Fóssil Gasolina ou Diesel (g/km)",
    16: "Emissões GEE - CO2e Fóssil VEHP Plug-in (g/km)",
    17: "Consumo Etanol - Cidade (km/l)",
    18: "Consumo Etanol - Estrada (km/l)",
    19: "Consumo Gasolina ou Diesel - Cidade (km/l)",
    20: "Consumo Gasolina ou Diesel - Estrada (km/l)",
    21: "Consumo Elétrico - Cidade (km/le)",
    22: "Consumo Elétrico - Estrada (km/le)",
    23: "Consumo Energético (MJ/km)",
    24: "Autonomia Modo Elétrico (km)",
    25: "Classificação PBE - Relativa na Categoria (A=Mais Eficiente / B / C / D / E=Menos Eficiente)",
    26: "Classificação PBE - Absoluta Geral (A=Mais Eficiente / B / C / D / E=Menos Eficiente)",
    27: "Selo CONPET de Eficiência Energética (SIM / NÃO)",
}

# Colunas que devem ser tratadas como número
COLUNAS_NUMERICAS = {10, 11, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}

# Categorias válidas do PBEV
CATEGORIAS = [
    'sub compacto', 'compacto', 'médio', 'grande', 'extra grande',
    'utilitário esportivo compacto', 'utilitário esportivo grande',
    'utilitário esportivo grande 4x4', 'fora de estrada compacto',
    'fora de estrada grande', 'minivan', 'comercial',
    'picape compacta', 'picape', 'esportivo',
]


# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def limpar(val):
    """Limpa valor de célula do PDF."""
    if val is None:
        return None
    v = str(val).strip().replace('\n', ' ')
    if v in ('', '\\', '-', '\\\\', "\\\\'", 'ND', 'N.A.', 'N/A', '--'):
        return None
    return v


def para_numero(val):
    """Converte string para float."""
    v = limpar(val)
    if v is None:
        return None
    v = v.replace(',', '.')
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def eh_cabecalho(row):
    """Detecta se a linha é cabeçalho do PDF (não dados)."""
    texto = ' '.join([str(c) for c in row if c]).lower()
    return any(kw in texto for kw in [
        'categoria', 'marca', 'modelo', 'versão', 'motor', 'transmissão',
        'combustível', 'poluentes', 'quilometragem', 'emissões',
        'classificação', 'programa brasileiro', 'hidráulica', 'mecânica',
        'eletro-hidráulica', 'manual (m)', 'automática', 'nmog+nox',
        'gás efeito', 'consumo energético', 'menores níveis',
        'maiores níveis', 'www.', 'inmetro', 'conpet', 'ibama',
        'tipo de', 'propulsão', 'comparação', 'relativa', 'absoluta',
        'autonomia', 'ar\ncond', 'direção', 'fóssil',
        'cidade\n(km', 'estrada\n(km', 'valores em km',
    ])


def eh_veiculo(row):
    """Detecta se a linha é um registro de veículo válido."""
    if not row or not row[0]:
        return False
    celula = str(row[0]).strip().lower()
    return any(cat in celula for cat in CATEGORIAS)


def detectar_ano(pdf_path):
    """Tenta detectar o ano do PBEV pelo nome do arquivo."""
    nome = os.path.basename(pdf_path)
    match = re.search(r'20\d{2}', nome)
    return match.group() if match else 'PBEV'


# ============================================================================
# EXTRAÇÃO
# ============================================================================

def extrair(pdf_path):
    """Extrai todas as linhas de dados do PDF."""
    print(f"📄 Abrindo: {pdf_path}")
    linhas = []

    with pdfplumber.open(pdf_path) as pdf:
        n = len(pdf.pages)
        print(f"📑 Páginas: {n}")

        for i, page in enumerate(pdf.pages):
            print(f"  Página {i+1}/{n}", end='\r')
            for table in page.extract_tables():
                if not table:
                    continue
                for row in table:
                    if row and len(row) >= 25:  # tolerância pra variação
                        if not eh_cabecalho(row) and eh_veiculo(row):
                            linhas.append(row)

    print(f"\n✅ Linhas de veículos extraídas: {len(linhas)}")
    return linhas


def parsear(linhas):
    """Converte linhas brutas em lista de dicionários."""
    registros = []
    n_colunas_esperadas = max(COLUNAS_PDF.keys()) + 1  # 28

    for row in linhas:
        # Normalizar tamanho
        while len(row) < n_colunas_esperadas:
            row.append(None)

        registro = {}
        for idx, nome in COLUNAS_PDF.items():
            if idx in COLUNAS_NUMERICAS:
                registro[nome] = para_numero(row[idx])
            else:
                registro[nome] = limpar(row[idx])

        if registro.get(COLUNAS_PDF[1]):  # tem Marca
            registros.append(registro)

    return registros


# ============================================================================
# PÓS-PROCESSAMENTO
# ============================================================================

def pos_processar(df):
    """Adiciona colunas derivadas para análise."""
    print("🔧 Pós-processando...")

    # Nomes curtos para referência interna
    col_prop = COLUNAS_PDF[5]
    col_trans = COLUNAS_PDF[6]
    col_comb = COLUNAS_PDF[9]
    col_selo = COLUNAS_PDF[27]

    # --- Propulsão padronizada ---
    mapa = {
        'combustão': 'Combustão', 'híbrido': 'Híbrido',
        'plug-in': 'Plug-in', 'elétrico': 'Elétrico',
        'hibrido': 'Híbrido', 'eletrico': 'Elétrico',
    }
    df[col_prop] = df[col_prop].str.lower().map(mapa).fillna(df[col_prop])

    # --- Transmissão: tipo e nº velocidades ---
    df['Transmissão - Tipo'] = df[col_trans].apply(
        lambda x: re.sub(r'[-\d]', '', str(x)).strip() if pd.notna(x) else None)
    df['Transmissão - Nº Velocidades'] = df[col_trans].apply(
        lambda x: int(re.search(r'(\d+)', str(x)).group(1))
        if pd.notna(x) and re.search(r'(\d+)', str(x)) else None)

    # --- Combustível por extenso ---
    mapa_c = {'E': 'Elétrico', 'G': 'Gasolina', 'F': 'Flex', 'D': 'Diesel'}
    df['Combustível - Descrição'] = df[col_comb].map(mapa_c).fillna(df[col_comb])

    # --- Consumo Combinado (55% cidade + 45% estrada - metodologia INMETRO) ---
    pares = [
        (COLUNAS_PDF[19], COLUNAS_PDF[20], 'Consumo Gasolina ou Diesel - Combinado (km/l)'),
        (COLUNAS_PDF[17], COLUNAS_PDF[18], 'Consumo Etanol - Combinado (km/l)'),
        (COLUNAS_PDF[21], COLUNAS_PDF[22], 'Consumo Elétrico - Combinado (km/le)'),
    ]
    for cid, est, nome_comb in pares:
        if cid in df.columns and est in df.columns:
            df[nome_comb] = (0.55 * df[cid] + 0.45 * df[est]).round(2)

    # --- Faixa de CO2 ---
    col_co2 = COLUNAS_PDF[15]
    bins = [0, 50, 100, 150, 200, 250, 999]
    labels = ['0-50', '51-100', '101-150', '151-200', '201-250', '250+']
    df['Faixa CO2 Gasolina/Diesel (g/km)'] = pd.cut(
        df[col_co2], bins=bins, labels=labels, include_lowest=True)

    # --- Flags ---
    df['Zero Emissão (Elétrico Puro)'] = df[col_prop] == 'Elétrico'
    df['Eletrificado (Elétrico/Híbrido/Plug-in)'] = df[col_prop].isin(
        ['Elétrico', 'Híbrido', 'Plug-in'])

    # --- Selo CONPET ---
    df[col_selo] = df[col_selo].apply(
        lambda x: 'SIM' if str(x).strip().upper() == 'SIM' else 'NÃO')

    return df


# ============================================================================
# EXPORTAÇÃO
# ============================================================================

def exportar(df, ano, pasta_saida):
    """Salva Excel (com abas de resumo e legendas) e CSV."""
    excel_path = os.path.join(pasta_saida, f'pbev_{ano}_dados.xlsx')
    csv_path = os.path.join(pasta_saida, f'pbev_{ano}_dados.csv')

    col_co2 = COLUNAS_PDF[15]
    col_ce = COLUNAS_PDF[23]
    col_prop = COLUNAS_PDF[5]
    col_comb_gas = 'Consumo Gasolina ou Diesel - Combinado (km/l)'

    print(f"\n💾 Salvando: {excel_path}")
    with pd.ExcelWriter(excel_path, engine='openpyxl') as w:

        # 1. Dados completos
        df.to_excel(w, sheet_name='Dados', index=False)

        # 2. Resumo por Propulsão
        df.groupby(col_prop).agg(
            Quantidade=('Marca', 'count'),
            CO2_Medio_gkm=(col_co2, 'mean'),
            CO2_Mediano_gkm=(col_co2, 'median'),
            Consumo_Energetico_Medio_MJkm=(col_ce, 'mean'),
        ).round(2).reset_index().to_excel(w, sheet_name='Resumo_Propulsão', index=False)

        # 3. Resumo por Marca
        rm = df.groupby('Marca').agg(
            Modelos=('Modelo', 'nunique'),
            Versões=('Versão', 'count'),
            CO2_Medio_gkm=(col_co2, 'mean'),
            Pct_Eletrificados=('Eletrificado (Elétrico/Híbrido/Plug-in)', 'mean'),
        ).round(2).reset_index()
        rm['Pct_Eletrificados'] = (rm['Pct_Eletrificados'] * 100).round(1)
        rm.sort_values('Versões', ascending=False).to_excel(
            w, sheet_name='Resumo_Marca', index=False)

        # 4. Resumo por Categoria
        df.groupby('Categoria').agg(
            Quantidade=('Marca', 'count'),
            CO2_Medio_gkm=(col_co2, 'mean'),
            Consumo_Gas_Combinado_kml=(col_comb_gas, 'mean'),
        ).round(2).reset_index().to_excel(
            w, sheet_name='Resumo_Categoria', index=False)

        # 5. Dicionário de colunas
        pd.DataFrame({
            'Nº': range(1, len(df.columns) + 1),
            'Coluna': list(df.columns),
            'Tipo': [str(df[c].dtype) for c in df.columns],
            'Preenchidos': [df[c].notna().sum() for c in df.columns],
            'Exemplo': [str(df[c].dropna().iloc[0]) if df[c].notna().any() else '' for c in df.columns],
        }).to_excel(w, sheet_name='Dicionário', index=False)

        # 6. LEGENDAS completas
        legendas = [
            # Transmissão
            ('Transmissão', 'M', 'Manual'),
            ('Transmissão', 'A', 'Automática'),
            ('Transmissão', 'DCT', 'Automática Dupla Embreagem'),
            ('Transmissão', 'MTA', 'Automatizada'),
            ('Transmissão', 'CVT', 'Contínua (Variação Contínua)'),
            ('Transmissão', 'Nº após hífen', 'Quantidade de marchas (ex: M-5 = Manual 5 marchas)'),
            ('Transmissão', 'N.A. ou --', 'Não se aplica (veículos elétricos)'),
            # Ar Cond
            ('Ar Condicionado', 'S', 'Sim'),
            ('Ar Condicionado', 'N', 'Não'),
            # Direção
            ('Direção Assistida', 'H', 'Hidráulica'),
            ('Direção Assistida', 'M', 'Mecânica'),
            ('Direção Assistida', 'E', 'Elétrica'),
            ('Direção Assistida', 'E-H', 'Eletro-hidráulica'),
            # Combustível
            ('Combustível', 'E', 'Elétrico'),
            ('Combustível', 'G', 'Gasolina'),
            ('Combustível', 'F', 'Flex (Etanol / Gasolina)'),
            ('Combustível', 'D', 'Diesel'),
            # Propulsão
            ('Tipo de Propulsão', 'Combustão', 'Motor a combustão interna'),
            ('Tipo de Propulsão', 'Híbrido', 'Combustão + elétrico (não recarregável na tomada)'),
            ('Tipo de Propulsão', 'Plug-in', 'Híbrido recarregável na tomada (VEHP)'),
            ('Tipo de Propulsão', 'Elétrico', '100% elétrico (VE) - zero emissão no escapamento'),
            # Emissões
            ('Emissões Poluentes', 'NMOG+NOx', 'Hidrocarbonetos não-metano + Óxidos de Nitrogênio'),
            ('Emissões Poluentes', 'CO', 'Monóxido de Carbono'),
            ('Emissões Poluentes', 'CHO', 'Aldeídos (formaldeído + acetaldeído)'),
            ('Emissões Poluentes', 'ND', 'Não Disponível (importados sem ensaio local)'),
            # Redução
            ('Redução Relativa', 'A', '≥ 40% abaixo do limite PROCONVE L7 (melhor)'),
            ('Redução Relativa', 'B', '< 40% abaixo do limite PROCONVE L7'),
            # GEE
            ('Emissões GEE', 'CO2 Fóssil Etanol', 'CO2 fóssil ao usar Etanol (0 para flex pois etanol é renovável)'),
            ('Emissões GEE', 'CO2 Fóssil Gasolina/Diesel', 'CO2 fóssil ao usar Gasolina ou Diesel'),
            ('Emissões GEE', 'CO2e VEHP', 'CO2 equivalente fóssil para Plug-in (modo combinado)'),
            # Consumo
            ('Consumo (km/l)', 'Cidade', 'Ciclo urbano (mais paradas)'),
            ('Consumo (km/l)', 'Estrada', 'Ciclo rodoviário (velocidade constante)'),
            ('Consumo (km/l)', 'Combinado', '55% cidade + 45% estrada (metodologia INMETRO)'),
            ('Consumo (km/l)', 'km/le', 'Quilômetros por litro de gasolina equivalente (1L ≈ 8,9 kWh)'),
            # Consumo Energético
            ('Consumo Energético', 'MJ/km', 'Megajoules por km — quanto menor, mais eficiente'),
            ('Consumo Energético', 'Referência', 'Elétrico ~0.4-0.7 | Combustão ~1.4-1.6 | SUV diesel ~2.5-3.0'),
            # Classificação PBE
            ('Classificação PBE', 'A', 'Mais eficiente (menor consumo energético)'),
            ('Classificação PBE', 'B', 'Eficiente'),
            ('Classificação PBE', 'C', 'Médio'),
            ('Classificação PBE', 'D', 'Menos eficiente'),
            ('Classificação PBE', 'E', 'Menos eficiente (maior consumo energético)'),
            ('Classificação PBE', 'Relativa na Categoria', 'Compara com outros veículos da mesma categoria'),
            ('Classificação PBE', 'Absoluta Geral', 'Compara com TODOS os veículos do programa'),
            # Selo
            ('Selo CONPET', 'SIM', 'Recebeu selo de eficiência energética'),
            ('Selo CONPET', 'NÃO', 'Não recebeu o selo'),
            # Categorias
            ('Categoria', 'Sub Compacto', 'Ex: Fiat Mobi, BYD Dolphin Mini'),
            ('Categoria', 'Compacto', 'Ex: Fiat Argo, Hyundai HB20'),
            ('Categoria', 'Médio', 'Ex: Audi A3, Toyota Corolla'),
            ('Categoria', 'Grande', 'Ex: Mercedes C300, BMW 330i'),
            ('Categoria', 'Extra Grande', 'Ex: BMW i4, Audi A6'),
            ('Categoria', 'Esportivo', 'Ex: Porsche 911, BMW M3'),
            ('Categoria', 'Utilitário Esportivo Compacto', 'SUVs compactos — ex: Creta, T-Cross'),
            ('Categoria', 'Utilitário Esportivo Grande', 'SUVs grandes — ex: Commander, SW4'),
            ('Categoria', 'Utilitário Esp. Grande 4x4', 'SUVs grandes com tração 4x4'),
            ('Categoria', 'Fora de Estrada Compacto', 'Off-road compactos'),
            ('Categoria', 'Fora de Estrada Grande', 'Off-road grandes — ex: Land Rover Defender'),
            ('Categoria', 'Picape', 'Ex: Fiat Toro, Toyota Hilux'),
            ('Categoria', 'Picape Compacta', 'Ex: Fiat Strada'),
            ('Categoria', 'Minivan', 'Minivans'),
            ('Categoria', 'Comercial', 'Veículos comerciais leves'),
            # Valores especiais
            ('Valores Especiais', '\\', 'Não se aplica ao veículo'),
            ('Valores Especiais', '-', 'Sem informação / Não possui'),
            ('Valores Especiais', 'ND', 'Não Disponível (ensaio não realizado)'),
            ('Valores Especiais', 'N.A.', 'Não Aplicável'),
            ('Valores Especiais', 'vazio / None', 'Campo vazio no PDF original'),
        ]
        pd.DataFrame(legendas, columns=['Campo', 'Código', 'Significado']).to_excel(
            w, sheet_name='Legendas', index=False)

    # CSV
    print(f"💾 Salvando: {csv_path}")
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')

    return excel_path, csv_path


# ============================================================================
# PIPELINE PRINCIPAL
# ============================================================================

def main(pdf_path=None):
    # Determinar PDF
    if pdf_path is None:
        pdf_path = sys.argv[1] if len(sys.argv) > 1 else PDF_PADRAO

    if not os.path.exists(pdf_path):
        print(f"❌ Arquivo não encontrado: {pdf_path}")
        sys.exit(1)

    ano = detectar_ano(pdf_path)
    pasta_saida = '../data/processed'

    print("=" * 70)
    print(f"  EXTRAÇÃO PBEV {ano}")
    print("=" * 70)

    # 1. Extrair
    linhas = extrair(pdf_path)

    # 2. Parsear
    print("🔄 Parseando...")
    registros = parsear(linhas)
    print(f"✅ Registros: {len(registros)}")

    # 3. DataFrame
    df = pd.DataFrame(registros)

    # 4. Tipos numéricos
    for idx in COLUNAS_NUMERICAS:
        col = COLUNAS_PDF[idx]
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 5. Pós-processar
    df = pos_processar(df)

    # 6. Duplicatas
    n = len(df)
    df = df.drop_duplicates()
    dup = n - len(df)
    if dup:
        print(f"🗑️  Duplicatas: {dup}")

    # 7. Ordenar
    df = df.sort_values(['Categoria', 'Marca', 'Modelo', 'Versão']).reset_index(drop=True)

    # 8. Relatório
    col_co2 = COLUNAS_PDF[15]
    col_prop = COLUNAS_PDF[5]
    co2 = df[col_co2].dropna()
    co2_comb = df[df[col_prop] == 'Combustão'][col_co2].dropna()

    print("\n" + "=" * 70)
    print(f"  📊 RELATÓRIO PBEV {ano}")
    print("=" * 70)
    print(f"  Registros: {len(df)}")
    print(f"  Colunas: {len(COLUNAS_PDF)} do PDF + {len(df.columns) - len(COLUNAS_PDF)} derivadas = {len(df.columns)}")
    print(f"  Marcas: {df['Marca'].nunique()} | Modelos: {df['Modelo'].nunique()}")

    print(f"\n  Propulsão:")
    for p, c in df[col_prop].value_counts().items():
        print(f"    {p:12s}: {c}")

    print(f"\n  CO2 Gasolina/Diesel:")
    print(f"    Todos:     média={co2.mean():.1f}  mediana={co2.median():.0f}  max={co2.max():.0f} g/km")
    if len(co2_comb):
        print(f"    Combustão: média={co2_comb.mean():.1f}  mediana={co2_comb.median():.0f} g/km")

    # 9. Exportar
    excel, csv = exportar(df, ano, pasta_saida)

    print(f"\n  📋 Colunas finais:")
    for i, c in enumerate(df.columns, 1):
        print(f"    {i:2d}. {c}")

    print("\n" + "=" * 70)
    print(f"  ✅ Pronto! Arquivos salvos:")
    print(f"     {excel}")
    print(f"     {csv}")
    print("=" * 70)

    return df


if __name__ == "__main__":
    df = main()