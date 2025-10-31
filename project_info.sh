cd ~/tg_uploader_bot_ver1
# Создать файл с информацией о проекте
echo "Project structure:" > project_analysis.txt
echo "=================" >> project_analysis.txt
ls -la >> project_analysis.txt
echo -e "\n\n" >> project_analysis.txt

# Добавить содержимое всех Python файлов с именами
for file in *.py; do
    echo "╔══════════════════════════════════════════════════════════════╗" >> project_analysis.txt
    echo "║                     FILE: $file" >> project_analysis.txt
    echo "╚══════════════════════════════════════════════════════════════╝" >> project_analysis.txt
    cat "$file" >> project_analysis.txt
    echo -e "\n\n" >> project_analysis.txt
done
