# Compiler flags
CC=g++-9
#Profiling CFLAGS
MATH_FLAGS=-ffast-math -funsafe-math-optimizations -fassociative-math -ffinite-math-only -fno-signed-zeros
CFLAGS=-march=core2 -msse4.1 -std=c++11 -c -O3 -W -Wall -Wextra -DDEBUG -DEXPBACKOFF -g -pedantic $(MATH_FLAGS) -funroll-all-loops -fvariable-expansion-in-unroller -fno-omit-frame-pointer -Iinclude
RELEASE_CFLAGS=-march=core2 -msse4.1 -std=c++11 -c -O3 -W -Wall -Wextra -DNDEBUG -DNDBGPRINT -DEXPBACKOFF -pedantic $(MATH_FLAGS) -funroll-all-loops -fvariable-expansion-in-unroller -Iinclude

#71, 1159
RELEASE_LDFLAGS=-Wl,-O1 -Wl,-wrap,malloc -Wl,-wrap,mmap -Wl,-wrap,posix_memalign -Wl,--whole-archive -lpthread -Wl,--no-whole-archive -static -static-libgcc
EXEC_LDFLAGS=-Wl,-O1 -Wl,-wrap,malloc -Wl,-wrap,mmap -Wl,-wrap,posix_memalign -pthread
LIBS=

# Source / Executable Variables
CORE_SOURCES= util/memoryhooks.cpp util/io.cpp util/chrono.cpp util/counters.cpp alloc.cpp indexes.cpp scheduler.cpp schedulegraph.cpp query1.cpp query2.cpp query3.cpp query4.cpp include/MurmurHash2.cpp include/MurmurHash3.cpp
ALL_SOURCES=tester.cpp main.cpp $(CORE_SOURCES)
CORE_OBJECTS=$(addsuffix .o, $(basename $(CORE_SOURCES)))
CORE_DEPS=$(addsuffix .depends, $(basename $(ALL_SOURCES)))

EXEC_EXECUTABLE=runGraphQueries
EXEC_TESTER_EXECUTABLE=runTester

RELEASE_OBJECTS=$(addsuffix .release.o, $(basename $(CORE_SOURCES)))

# Test env variables
ifndef FACTOR
	FACTOR=1
endif
ifndef EXCLUDE
	EXCLUDE="nothing"
endif
TEST_DATA_PATH=test_data
100K_DATASET_PATH=$(TEST_DATA_PATH)/data100k
ADDITIONAL_TEST_DATA_PATH=test_queries
10K_DATASET_URL=http://www.cs.albany.edu/~sigmod14contest/files/outputDir-10k.zip
10K_QUERIES_URL=http://www.cs.albany.edu/~sigmod14contest/files/10k-queries.txt
10K_ANSWERS_URL=http://www.cs.albany.edu/~sigmod14contest/files/10k-answers.txt
10K_DATASET_PATH=$(TEST_DATA_PATH)/data10k
1K_DATASET_URL=http://www.cs.albany.edu/~sigmod14contest/files/outputDir-1k.zip
1K_QUERIES_URL=http://www.cs.albany.edu/~sigmod14contest/files/1k-queries.txt
1K_ANSWERS_URL=http://www.cs.albany.edu/~sigmod14contest/files/1k-answers.txt
1K_DATASET_PATH=$(TEST_DATA_PATH)/data1k
TEST_DATA_FILES=$(10K_DATASET_PATH) $(1K_DATASET_PATH) $(10K_DATASET_PATH) $(TEST_DATA_PATH)/1k-queries.txt $(TEST_DATA_PATH)/1k-answers.txt $(TEST_DATA_PATH)/10k-queries.txt $(TEST_DATA_PATH)/10k-answers.txt 

# Program rules
.PHONY: test_env test_all executables

all: executables
	@rm -f $(CORE_DEPS)

test_all: test_1k test_10k

test_100k: test_env $(100K_DATASET_PATH)  $(EXEC_TESTER_EXECUTABLE)
	./runTester -factor $(FACTOR) -exclude $(EXCLUDE) $(TEST_DATA_PATH)/data100k/ test_queries/additional-100k-queries.txt test_queries/additional-100k-answers.txt

test_10k: test_env $(EXEC_TESTER_EXECUTABLE)
	./runTester -factor $(FACTOR) -exclude $(EXCLUDE) $(TEST_DATA_PATH)/data10k/ $(TEST_DATA_PATH)/10k-queries.txt $(TEST_DATA_PATH)/10k-answers.txt

test_1k: test_env $(EXEC_TESTER_EXECUTABLE)
	./runTester -factor $(FACTOR) -exclude $(EXCLUDE) $(TEST_DATA_PATH)/data1k/ $(TEST_DATA_PATH)/1k-queries.txt $(TEST_DATA_PATH)/1k-answers.txt
	./runTester -factor $(FACTOR) -exclude $(EXCLUDE) $(TEST_DATA_PATH)/data1k/ $(ADDITIONAL_TEST_DATA_PATH)/additional-1k-queries.txt $(ADDITIONAL_TEST_DATA_PATH)/additional-1k-answers.txt

clean:
	-rm $(EXEC_EXECUTABLE) $(EXEC_TESTER_EXECUTABLE)
	-rm *.o util/*.o
	-rm $(CORE_DEPS)

executables: $(EXEC_EXECUTABLE) $(EXEC_TESTER_EXECUTABLE)
	@rm $(CORE_DEPS)

$(EXEC_TESTER_EXECUTABLE): tester.o $(CORE_OBJECTS)
	$(CC) tester.o -DDEBUG -g $(CORE_OBJECTS) -o $@ $(EXEC_LDFLAGS) $(LIBS)

$(EXEC_EXECUTABLE): main.release.o $(RELEASE_OBJECTS)
	$(CC) main.release.o $(RELEASE_OBJECTS) -o $@ $(RELEASE_LDFLAGS) $(LIBS)

%.release.o: %.cpp
	$(CC) $(RELEASE_CFLAGS) $< -o $@ $(LIBS)

.cpp.o:
	$(CC) $(CFLAGS) $< -o $@ $(LIBS)


%.depends: %.cpp
	@$(CC) -M $(CFLAGS) $< > $@ $(LIBS)

# Test data rules
test_env: $(TEST_DATA_PATH) $(TEST_DATA_FILES)

$(TEST_DATA_PATH):
	mkdir $(TEST_DATA_PATH)

test_data/data10k:
	cd $(TEST_DATA_PATH); rm -f outputDir-10k.zi*; wget $(10K_DATASET_URL)
	cd $(TEST_DATA_PATH); unzip -d tmp10k -o outputDir-10k.zip; rm -fr tmp10k/__MACOSX outputDir-10k.zip
	cd $(TEST_DATA_PATH); mv tmp10k data10k

test_data/data1k:
	cd $(TEST_DATA_PATH); rm -f outputDir-1k.zi*;  wget $(1K_DATASET_URL)
	cd $(TEST_DATA_PATH); unzip outputDir-1k.zip; rm -fr __MACOSX outputDir-1k.zip
	cd $(TEST_DATA_PATH); mv outputDir-1k data1k

test_data/10k-queries.txt:
	cd $(TEST_DATA_PATH); wget $(10K_QUERIES_URL)

test_data/10k-answers.txt:
	cd $(TEST_DATA_PATH); wget $(10K_ANSWERS_URL)

test_data/1k-queries.txt:
	cd $(TEST_DATA_PATH); wget $(1K_QUERIES_URL)

test_data/1k-answers.txt:
	cd $(TEST_DATA_PATH); wget $(1K_ANSWERS_URL)

-include $(CORE_DEPS)
