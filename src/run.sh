#! /bin/bash

RES_FILE="res.txt"

INPUT_DIR="../data/data"
OUTPUT_DIR="../data/sorted"
EXTENSION_DATA_FILE="txt"

DEBUG_MODE=0

# Record & plot
# psrecord $(pgrep mysort) --interval 1 --plot $PLOTS_DIR/plot_mysort_$size.png


# Empty res.txt
echo '' > $RES_FILE

# Compile prog
echo -e "================ COMPILING ================\n"
make clean; make &> /dev/null
echo -e "\nCompiling done\n\n"



function runSort() {
	input_file=$1
	output_file=$2
	num_threads=$3
	max_memory=$4
	debug_mode=$5

	bin/./mysort $input_file $output_file $num_threads $max_memory $debug_mode
}

function runLinuxSort() {
	input_file=$1
	output_file=$2
	num_threads=$3

	time sort --parallel=$num_threads -k 1 $input_file -o $output_file
}

function testSorted() {
	input_file=$1

	data/gensort/./valsort $input_file
}

function run() {
	size=$1
	num_threads=$2
	max_memory=$3
	debug_mode=$4

	input_file="$INPUT_DIR/data_$size.$EXTENSION_DATA_FILE"
	output_file="$OUTPUT_DIR/sorted_$size.$EXTENSION_DATA_FILE"

	echo -e "\n--------------------- Size: $size GB; Max memory: $max_memory GB ---------------------\n"
	# Run shared sort
	runSort $input_file $output_file  $num_threads $max_memory $debug_mode

	# Validation
	echo -e "\nValidation:"
	testSorted $output_file

	# # Linux sort
	echo -e "\nLinux sort:"
	runLinuxSort $input_file $output_file $num_threads
}



echo -e "================ RUNNING ================\n\n"


{ run 1 7 2 $DEBUG_MODE ; } >> $RES_FILE 2>> $RES_FILE
{ run 4 8 5 $DEBUG_MODE ; } >> $RES_FILE 2>> $RES_FILE
{ run 16 8 5 $DEBUG_MODE ; } >> $RES_FILE 2>> $RES_FILE

# { run 32 8 5 $DEBUG_MODE ; } >> $RES_FILE 2>> $RES_FILE

