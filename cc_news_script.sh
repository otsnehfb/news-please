# usage: cat cc_news_2019.paths | ./cc_news_script.sh
count=1
BASE_PATH="$(pwd)"

mkdir "$BASE_PATH/extracted_files"
while read -r line; do
    echo "Begin extracting file $count: $line"
    python3 -m newsplease.examples.commoncrawl --warc-file "$line" \
        --outfile "$BASE_PATH/extracted_files/extracted_${line##*/}" \
        --download-dir "$BASE_PATH/cc_news_tmp"
    echo "Done extracting file $count: $line"
    exit 1
    (( count++ ))
done
