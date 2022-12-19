#include <algorithm>
#include <omp.h>
// Additional libraries used:
// https://github.com/nlohmann/json
#include <nlohmann/json.hpp>
// https://github.com/vog/sha1
#include "sha1-master/sha1.hpp"

using namespace std;
using json = nlohmann::json;

const string INPUT_FILE = "input.json";
const string OUTPUT_FILE = "output.txt";

const int THREAD_NUM = 4;
const int FILTER_FROM_YEAR = 2000; // Data range [1964;2021]

struct Car {
    string name;
    int year;
    double mileage;
    string hash;

    Car(string _name = "", int _year = 0, double _mileage = 0.0, string _hash = "") {
        name = _name;
        year = _year;
        mileage = _mileage;
        hash = _hash;
    }

    bool operator== (const Car& _car) {
        return (!name.compare(_car.name) && year == _car.year
                && mileage == _car.mileage && !hash.compare(_car.hash));
    }
};

vector<Car> readData();
void splitElements(int arr[], int dataSize);
void task(Car& car);
void insertSorted(vector<Car> &cars, Car car);
void writeInitialData(vector<Car> cars);
void writeRes(vector<Car> cars, int sumYear, double sumMileage);

int main() {
    vector<Car> cars = readData();
    vector<Car> resCars;
    resCars.reserve(cars.size());
    int sumYear = 0;
    double sumMileage = 0;

    int endParts[THREAD_NUM];
    splitElements(endParts, cars.size());

    omp_set_num_threads(THREAD_NUM);
#pragma omp parallel shared(cars, resCars) default(none) firstprivate(endParts) reduction(+:sumYear, sumMileage)
    {
        int idx = omp_get_thread_num();
        int start = (idx == 0) ? 0 : endParts[idx - 1];
        int end = (idx == THREAD_NUM - 1) ? cars.size() : endParts[idx];

        for (int i = start; i < end; i++) {
            task(cars[i]);
            if (FILTER_FROM_YEAR > cars[i].year) {
                continue;
            }
#pragma omp critical
            {
                insertSorted(resCars, cars[i]);
                sumYear += cars[i].year;
                sumMileage += cars[i].mileage;
            }
        }
    }

    writeInitialData(cars);
    writeRes(resCars, sumYear, sumMileage);

    return 0;
}

vector<Car> readData() {
    vector<Car> cars;

    ifstream f(INPUT_FILE);
    json fileData = json::parse(f);

    json carsData = fileData["cars"];
    for (int i = 0; i < carsData.size(); i++) {

        Car newCar = {
            carsData[i]["name"],
            carsData[i]["year"],
            carsData[i]["mileage"],
        };

        cars.push_back(newCar);
    }

    return cars;
}

void splitElements(int arr[], int dataSize) {
    int floor = dataSize / THREAD_NUM;
    int rem = dataSize % THREAD_NUM;

    arr[0] = 0;
    for (int i = 1; i < THREAD_NUM; i++) {
        if (rem > 0) {
            arr[i] = floor + 1 + arr[i - 1];
            rem--;
        } else {
            arr[i] = floor + arr[i - 1];
        }
    }
}

void task(Car& car) {
    SHA1 sha;
    sha.update(car.name + to_string(car.year) + to_string(car.mileage));
    car.hash = sha.final();
}

void insertSorted(vector<Car> &cars, Car car)
{
    vector<Car>::iterator it;

    for (it = cars.begin(); it < cars.end(); it++) {
        if (it->hash.compare(car.hash) > 0) {
            cars.insert(it, car);
            return;
        }
    }

    cars.push_back(car);
}

void writeInitialData(vector<Car> cars) {
    const int NameWidth = 25;
    const int YearWidth = 5;
    const int MileageWidth = 10;
    const int HashWidth = 4;
    const char HorizontalSeparator = '-';
    const int HLength = NameWidth + YearWidth + MileageWidth + HashWidth + 11;
    const char* VerticalSeparator = " | ";

    ofstream output(OUTPUT_FILE);

    output << left << "Initial data" << endl;
    output << string(HLength, HorizontalSeparator) << endl;
    output << right << setw(NameWidth) << "Name" << VerticalSeparator
        << setw(YearWidth) << "Year" << VerticalSeparator
        << setw(MileageWidth) << "Mileage" << VerticalSeparator
        << setw(HashWidth) << "Hash" << VerticalSeparator << endl;
    output << string(HLength, HorizontalSeparator) << endl;
    for (int i = 0; i < cars.size(); i++)
    {
        Car car = cars[i];
        output << right << setw(NameWidth) << car.name << VerticalSeparator << setw(YearWidth)
            << car.year << VerticalSeparator << fixed << setprecision(2) << setw(MileageWidth)
            << car.mileage << VerticalSeparator << setw(HashWidth) << ""
            << VerticalSeparator << endl;
    }
    output << string(HLength, HorizontalSeparator) << endl;

    output.close();
}

void writeRes(vector<Car> cars, int sumYear, double sumMileage) {
    const int NameWidth = 25;
    const int YearWidth = 5;
    const int MileageWidth = 10;
    const int HashWidth = 41;
    const char HorizontalSeparator = '-';
    const int HLength = NameWidth + YearWidth + MileageWidth + HashWidth + 11;
    const char* VerticalSeparator = " | ";

    ofstream output(OUTPUT_FILE, ios_base::app);

    output << left << "Results" << endl;
    output << string(HLength, HorizontalSeparator) << endl;
    output << right << setw(NameWidth) << "Name" << VerticalSeparator
        << setw(YearWidth) << "Year" << VerticalSeparator
        << setw(MileageWidth) << "Mileage" << VerticalSeparator
        << setw(HashWidth) << "Hash" << VerticalSeparator << endl;
    output << string(HLength, HorizontalSeparator) << endl;
    for (int i = 0; i < cars.size(); i++)
    {
        Car car = cars[i];
        output << right << setw(NameWidth) << car.name << VerticalSeparator << setw(YearWidth)
            << car.year << VerticalSeparator << fixed << setprecision(2) << setw(MileageWidth)
            << car.mileage << VerticalSeparator << setw(HashWidth) << car.hash
            << VerticalSeparator << endl;
    }
    output << string(HLength, HorizontalSeparator) << endl;
    output << "Year sum: " << sumYear << ", mileage sum: " << sumMileage << endl;

    output.close();
}
