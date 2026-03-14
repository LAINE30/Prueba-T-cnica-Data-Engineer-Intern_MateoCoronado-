import subprocess

dataset = "olistbr/brazilian-ecommerce"

subprocess.run(
    ["kaggle", "datasets", "download", "-d", dataset, "-p", "data/raw", "--unzip"]
)

print("Dataset descargado correctamente")