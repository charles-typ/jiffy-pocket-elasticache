
max=`awk 'BEGIN{a=   0}{if ($2>0+a) a=$2} END{print a}' $1`
echo $max
