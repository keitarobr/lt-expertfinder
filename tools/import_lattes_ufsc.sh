while IFS= read -r line; do
  rm profiles/$line*.xml
  unzip $2/$line/curriculum.zip -d profiles/
  python import_lattes.py --xpertfinderhost=192.168.42.201 --lchost=192.168.42.201 --crossrefhost=192.168.42.201 --elhost=192.168.42.201 --profile_xml_file_path=profiles/$line.xml import_profile > logs/$line.log 2>&1 &
done < "$1"

