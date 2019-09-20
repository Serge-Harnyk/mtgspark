cat AllCards.json | jq -r 'to_entries[] | [.value]' | jq -sc '.|.[]' > AllCardsSplitted.json

cat AllCardsSplitted.json | sed 's/^.//' | sed 's/.$//'  > AllCardsCutted.json