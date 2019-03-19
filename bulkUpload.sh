#!/bin/bash
for i in `ls ./Preguntas_json`; do
	echo item: $i
	./partUploader.py --cfg ./cfg --verbose --type GeoJson --multiprocess --title $i Preguntas_json/$i

done
        

