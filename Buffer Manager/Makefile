all: expr assign3

expr:
	 gcc -o test_expr test_expr.c rm_serializer.c expr.c record_mgr.c buffer_mgr.c buffer_mgr_stat.c storage_mgr.c dberror.c -lm

assign3:
	 gcc -o test_assign3 test_assign3_1.c expr.c rm_serializer.c record_mgr.c buffer_mgr.c buffer_mgr_stat.c storage_mgr.c dberror.c -lm 

clean:
	rm -rf ./test_assign1 ./test_assign2 ./testbuffer.bin ./docs ./test_assign3 ./test_expr ./test_table_r
