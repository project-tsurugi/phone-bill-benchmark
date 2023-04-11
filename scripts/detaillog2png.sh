#!/bin/bash -xeu

# 引数の確認

if [ $# -ne 1 ]; then
  echo "Error: Invalid number of arguments. Please specify only one argument."
  exit 1
fi

# 入出力ファイル名の設定

input_file="$1"
filename=$(basename "$input_file")
directory=$(dirname "$input_file")
filename="${filename%.*}"
output_file=$directory/$filename.png

# テンポラリディレクトリを作成する
tempdir=$(mktemp -d)

# テンポラリディレクトリが正常に作成されたかどうかを確認する
if [ ! -d "$tempdir" ]; then
  echo "Error: Failed to create temporary directory."
  exit 1
fi


awk '
BEGIN { OFS="," ; print "label", "timestamp", "TID", "nRecords", "exec time(us)", "commit time(us)"}
/TIME INFO/{
        gsub(",","", $15);
        gsub(",", "", $19);
        gsub(",", "", $23);
        print LABEL, $1, $15, $19, $23, $27
}
/Using/{gsub(".*/", "",$NF);LABEL=$NF}
' "$input_file" > $tempdir/csv

set +e
labels=`cut -d ',' -f 1 $tempdir/csv | tail -n +2 | sort -u | grep OCC | grep -v online`
echo $labels
if [ -z "$labels" ] ; then
  rm -r "$tempdir"
  exit 0
fi
set -e

plotcmd="plot"
sp=" "
for label in $labels; do
  awk 'BEGIN{FS=","}/'$label'/{print $4, $5}' $tempdir/csv > $tempdir/$label
  plotcmd="$plotcmd"$sp'"'$label'"'
  sp=", "
done

cat > $tempdir/gp << EOF
set title "Analysis of exec time in relation to record count"
set key right bottom
set ylabel "exec time(us)"
set xlabel "updated records"
set logscale
set format x "10^{%L}"
set format y "10^{%L}"
set terminal png size 1280, 960
set output "$output_file" 
$plotcmd
set output
EOF
  
cp $tempdir/gp  /tmp/gp
cat $tempdir/gp
(
  cd $tempdir
  gnuplot gp
)


ls -l  $tempdir
# テンポラリディレクトリを削除する
rm -r "$tempdir"



