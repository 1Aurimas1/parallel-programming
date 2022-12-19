#include "cuda_runtime.h"
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <iomanip>
// Additional libraries used:
// https://github.com/nlohmann/json
#include "json.hpp"

#define INPUT_FILE "./input.json"
#define OUTPUT_FILE "./output.txt"

using namespace std;
using json = nlohmann::json;

const int MAX_CHAR_LENGTH = 200;
const int THREADS = 32;
const int BLOCKS = 2;

struct Car {
    char name[MAX_CHAR_LENGTH];
    int year;
    double mileage;
	int calculatedValue;

	void from_json(json data) {
		string tempName = data["name"];

		strncpy(name, tempName.c_str(), sizeof(name));
		name[sizeof(name) - 1] = 0;
		year = data["year"];
		mileage = data["mileage"];
	}
};


vector<Car> readFile() {
    vector<Car> cars;

    ifstream f(INPUT_FILE);
    json fileData = json::parse(f);

    json carsData = fileData["cars"];
    for (int i = 0; i < carsData.size(); i++) {
		Car tempCar;
		tempCar.from_json(carsData[i]);
        cars.push_back(tempCar);
    }

    return cars;
}

__device__ void gpuMemset(char* dest) {
    for (int i = 0; i < MAX_CHAR_LENGTH; ++i) {
        dest[i] = 0;
    }
}

__device__ void gpuStrcat(char* src, char* dest) {
    for (int i = 0; i < MAX_CHAR_LENGTH; ++i) {
        if (dest[i] == 0) {
            for (int j = 0; j < MAX_CHAR_LENGTH; ++j) {
                if (src[j] != 0) {
                    dest[i + j] = src[j];
                }
            }
            break;
        }
    }
}

__global__ void runOnGpu(Car* cars, int* n, int* blockSize, int* chunkSize, Car* results, int* resSize) {
	int startIdx = (blockIdx.x * *blockSize) + threadIdx.x * *chunkSize;
	int endIdx = ((blockIdx.x) * *blockSize) + (threadIdx.x + 1) * *chunkSize;

	if (endIdx + (*chunkSize * BLOCKS) >= *n) {
        endIdx = *n;
    }

    for (int i = startIdx; i < endIdx; i++) {
		Car resultCar;
		gpuMemset(resultCar.name);
		gpuStrcat(cars[i].name, resultCar.name);
		resultCar.year = cars[i].year;
		resultCar.mileage = cars[i].mileage;
		
		int year = resultCar.year;
		year *= 100;
		if (year >= resultCar.mileage) {
			resultCar.calculatedValue = year;
			int idx = atomicAdd(resSize, 1);
			results[idx] = resultCar;
		}
    } 
}

void writeResults(Car* results, int size) {
    const int NameWidth = 25;
    const int YearWidth = 5;
    const int MileageWidth = 10;
    const int ValWidth = 16;
    const char HorizontalSeparator = '-';
    const int HLength = NameWidth + YearWidth + MileageWidth + ValWidth + 11;
    const char* VerticalSeparator = " | ";

    ofstream output(OUTPUT_FILE);

    output << left << "Results" << endl;
    output << string(HLength, HorizontalSeparator) << endl;
    output << right << setw(NameWidth) << "Name" << VerticalSeparator
        << setw(YearWidth) << "Year" << VerticalSeparator
        << setw(MileageWidth) << "Mileage" << VerticalSeparator
        << setw(ValWidth) << "Calculated value" << VerticalSeparator << endl;
    output << string(HLength, HorizontalSeparator) << endl;
    for (int i = 0; i < size; i++)
    {
        Car car = results[i];
        output << right << setw(NameWidth) << car.name << VerticalSeparator << setw(YearWidth)
            << car.year << VerticalSeparator << fixed << setprecision(2) << setw(MileageWidth)
            << car.mileage << VerticalSeparator << setw(ValWidth) << car.calculatedValue
            << VerticalSeparator << endl;
    }
    output << string(HLength, HorizontalSeparator) << endl;

    output.close();
}


int main() {

	vector<Car> inputVec = readFile();

	Car* cars = &inputVec[0];
	int arrSize = inputVec.size();
	int resSize = 0;

	Car results[arrSize];
	int chunkSizePerBlock = arrSize / BLOCKS / THREADS;
	int blockSize = arrSize / BLOCKS;

	Car* deviceCars;
	Car* deviceResults;
	int* deviceN;
	int* deviceChunkSize;
	int* deviceBlockSize;
	int* deviceResultsN;
	
	cudaMalloc((void**)&deviceCars, arrSize * sizeof(Car));
	cudaMalloc((void**)&deviceResults, arrSize * sizeof(Car));
	cudaMalloc((void**)&deviceN, sizeof(int));
	cudaMalloc((void**)&deviceChunkSize, sizeof(int));
	cudaMalloc((void**)&deviceBlockSize, sizeof(int));
	cudaMalloc((void**)&deviceResultsN, sizeof(int));
	
	cudaMemcpy(deviceCars, cars, arrSize * sizeof(Car), cudaMemcpyHostToDevice);
	cudaMemcpy(deviceN, &arrSize, sizeof(int), cudaMemcpyHostToDevice);
	cudaMemcpy(deviceChunkSize, &chunkSizePerBlock, sizeof(int), cudaMemcpyHostToDevice);
	cudaMemcpy(deviceBlockSize, &blockSize, sizeof(int), cudaMemcpyHostToDevice);
	cudaMemcpy(deviceResultsN, &resSize, sizeof(int), cudaMemcpyHostToDevice);
	
	runOnGpu<<<BLOCKS, THREADS>>>(deviceCars, deviceN, deviceBlockSize, deviceChunkSize, deviceResults, deviceResultsN);
	cudaDeviceSynchronize();
	
	cudaMemcpy(&results, deviceResults, arrSize * sizeof(Car), cudaMemcpyDeviceToHost);
	cudaMemcpy(&resSize, deviceResultsN, sizeof(int), cudaMemcpyDeviceToHost);
	cudaFree(deviceCars);
	cudaFree(deviceN);
	cudaFree(deviceChunkSize);
	cudaFree(deviceResults);
	cudaFree(deviceBlockSize);

    cudaDeviceSynchronize();

	writeResults(results, resSize);
	
	return 0;
}