name: Comparar Yahoo x EODHD

on:
  workflow_dispatch:

jobs:
  comparar:
    runs-on: ubuntu-latest
    timeout-minutes: 20

    steps:
      - uses: actions/checkout@v6

      - uses: actions/setup-python@v6
        with:
          python-version: "3.11"
          cache: pip

      - name: Instala dependencias
        run: pip install -r requirements.txt

      - name: Compara as duas fontes
        env:
          EODHD_API_KEY: ${{ secrets.EODHD_API_KEY }}
        run: python comparar_fontes.py
