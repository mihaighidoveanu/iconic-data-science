FILES_DIR='/media/bob/Seagate Backup Plus Drive/MAG/graph/2018-09-27/'
EXPORT_DIR=/home/bob/gtd/Iconic/data/samples/

for d in "$FILES_DIR"*
do
	echo $d
	shuf -n 1000 $d > "$EXPORT_DIR/$d"
done
