#include <mpi.h>
// Additional libraries used:
// https://github.com/nlohmann/json
#include "json.hpp"
// https://github.com/vog/sha1
#include "sha1-master/sha1.hpp"

using namespace std;
using json = nlohmann::json;

const string INPUT_FILE = "input.json";
const string OUTPUT_FILE = "output.txt";

const int FILTER_FROM_YEAR = 2000; // File year range [1964;2021]

struct Car {
    string name;
    int year;
    double mileage;
    string hash;

    Car(string _name = "", int _year = 0, double _mileage = 0.0,
            string _hash = "") {
        name = _name;
        year = _year;
        mileage = _mileage;
        hash = _hash;
    }

    string to_json() {
        json j;
        j["name"] = name;
        j["year"] = year;
        j["mileage"] = mileage;
        j["hash"] = hash;
        return j.dump();
    }

    static Car from_json(const string &json_string) {
        auto parsed = json::parse(json_string);
        return {parsed["name"], parsed["year"], parsed["mileage"], parsed["hash"]};
    }

    bool operator==(const Car &_car) {
        return (!name.compare(_car.name) && year == _car.year &&
                mileage == _car.mileage && !hash.compare(_car.hash));
    }

    bool operator!=(const Car &_car) {
        return (name.compare(_car.name) || year != _car.year ||
                mileage != _car.mileage || hash.compare(_car.hash));
    }
};

const int MAIN_PROC = 0;
const int DATA_PROC = 1;
const int RESULT_PROC = 2;
const int WORKER_PROC = 3;

const int TO_DATA = 10;
const int TO_MAIN = 20;
const int TO_WORKER = 30;
const int TO_RES = 40;

const int INSERT = 100;
const int REMOVE = 101;
const int END = 102;
const int ACCEPT = 200;
const int REJECT = 201;
const int REQUEST_SIZE = 1;

vector<Car> readData();
void mainProcess(vector<Car> cars, vector<Car> results);
void dataProcess(int bufSize, const int workerCount);
void resultProcess(int bufSize, const int workerCount);
void workerProcess();
void computeHash(Car &car);
void insertItem(Car *cars, int &bufSize, Car item);
void writeInitialData(vector<Car> cars);
void writeRes(vector<Car> cars);

int main() {
    vector<Car> cars = readData();
    vector<Car> results;
    results.reserve(cars.size());

    MPI::Init();
    int rank = MPI::COMM_WORLD.Get_rank();
    if (MPI::COMM_WORLD.Get_size() < 4) {
        if (rank == MAIN_PROC) {
            cout << "Not enough threads for workers" << endl;
        }
        MPI::Finalize();
        return 1;
    }

    const int workerCount = MPI::COMM_WORLD.Get_size() - 3;
    if (rank == MAIN_PROC) {
        mainProcess(cars, results);
    } else if (rank == DATA_PROC) {
        dataProcess(cars.size() / 2, workerCount);
    } else if (rank == RESULT_PROC) {
        resultProcess(cars.size(), workerCount);
    } else if (rank >= WORKER_PROC) {
        workerProcess();
    }

    MPI::Finalize();

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

void mainProcess(vector<Car> cars, vector<Car> results) {
    writeInitialData(cars);

    for (int i = 0; i < cars.size();) {
        int request = INSERT;
        MPI::COMM_WORLD.Send(&request, REQUEST_SIZE, MPI::INT, DATA_PROC, TO_DATA);
        MPI::COMM_WORLD.Recv(&request, REQUEST_SIZE, MPI::INT, DATA_PROC, TO_MAIN);
        if (request != ACCEPT) {
            continue;
        }

        string serialized = cars[i].to_json();
        int entrySize = serialized.size();
        const char *entry = serialized.c_str();
        MPI::COMM_WORLD.Send(entry, entrySize, MPI::CHAR, DATA_PROC, TO_DATA);
        i++;
    }
    MPI::COMM_WORLD.Send(&END, REQUEST_SIZE, MPI::INT, DATA_PROC, TO_DATA);

    while (true) {
        MPI::Status status;
        MPI::COMM_WORLD.Probe(RESULT_PROC, TO_MAIN, status);
        int entrySize = status.Get_count(MPI::CHAR);
        char entry[entrySize];
        MPI::COMM_WORLD.Recv(entry, entrySize, MPI::CHAR, RESULT_PROC, TO_MAIN);
        if (entrySize == 0) {
            break;
        }

        Car car = Car::from_json(string(entry, entry + entrySize));
        results.push_back(car);
    }
    writeRes(results);
}

void dataProcess(int bufSize, const int workerCount) {
    Car *buffer = new Car[bufSize];
    int count = 0;
    bool isDataSending = true;
    while (isDataSending || count > 0) {
        MPI::Status status;
        MPI::COMM_WORLD.Probe(MPI::ANY_SOURCE, TO_DATA, status);
        int source = status.Get_source();
        int request;
        MPI::COMM_WORLD.Recv(&request, REQUEST_SIZE, MPI::INT, source, TO_DATA);

        if (request == INSERT) {
            if (count < bufSize) {
                MPI::COMM_WORLD.Send(&ACCEPT, REQUEST_SIZE, MPI::INT, source, TO_MAIN);

                MPI::COMM_WORLD.Probe(source, TO_DATA, status);
                int entrySize = status.Get_count(MPI::CHAR);
                char entry[entrySize];
                MPI::COMM_WORLD.Recv(entry, entrySize, MPI::CHAR, source, TO_DATA);

                Car car = Car::from_json(string(entry, entry + entrySize));
                buffer[count++] = car;
            } else {
                MPI::COMM_WORLD.Send(&REJECT, REQUEST_SIZE, MPI::INT, source, TO_MAIN);
            }
        } else if (request == REMOVE) {
            if (count > 0) {
                MPI::COMM_WORLD.Send(&ACCEPT, REQUEST_SIZE, MPI::INT, source,
                        TO_WORKER);

                string serialized = buffer[--count].to_json();
                int entrySize = serialized.size();
                const char *entry = serialized.c_str();

                MPI::COMM_WORLD.Send(entry, entrySize, MPI::CHAR, source, TO_WORKER);
            } else {
                MPI::COMM_WORLD.Send(&REJECT, REQUEST_SIZE, MPI::INT, source,
                        TO_WORKER);
            }
        } else if (request == END) {
            isDataSending = false;
        }
    }

    for (int i = 0; i < workerCount; i++) {
        MPI::COMM_WORLD.Send(&END, REQUEST_SIZE, MPI::INT, WORKER_PROC + i,
                TO_WORKER);
    }

    delete[] buffer;
}

void resultProcess(int bufSize, const int workerCount) {
    Car *buffer = new Car[bufSize];
    int count = 0;
    int workersFinished = 0;

    while (true) {
        MPI::Status status;
        MPI::COMM_WORLD.Probe(MPI::ANY_SOURCE, TO_RES, status);
        int entrySize = status.Get_count(MPI::CHAR);
        int source = status.Get_source();
        if (entrySize != 0) {
            char entry[entrySize];
            MPI::COMM_WORLD.Recv(entry, entrySize, MPI::CHAR, source, TO_RES);
            Car car = Car::from_json(string(entry, entry + entrySize));
            insertItem(buffer, count, car);
        } else if (entrySize == 0) {
            MPI::COMM_WORLD.Recv(NULL, 0, MPI::CHAR, source, TO_RES);
            if (++workersFinished >= workerCount) {
                break;
            }
        }
    }

    for (int i = 0; i < count; i++) {
        string serialized = buffer[i].to_json();
        int entrySize = serialized.size();
        const char *entry = serialized.c_str();
        MPI::COMM_WORLD.Send(entry, entrySize, MPI::CHAR, MAIN_PROC, TO_MAIN);
    }
    MPI::COMM_WORLD.Send(0, 0, MPI::CHAR, MAIN_PROC, TO_MAIN);

    delete[] buffer;
}

void workerProcess() {
    while (true) {
        int request = REMOVE;
        MPI::COMM_WORLD.Send(&request, REQUEST_SIZE, MPI::INT, DATA_PROC, TO_DATA);
        MPI::COMM_WORLD.Recv(&request, REQUEST_SIZE, MPI::INT, DATA_PROC,
                TO_WORKER);
        if (request == END) {
            MPI::COMM_WORLD.Send(0, 0, MPI::CHAR, RESULT_PROC, TO_RES);
            break;
        } else if (request != ACCEPT) {
            continue;
        }

        MPI::Status status;
        MPI::COMM_WORLD.Probe(DATA_PROC, TO_WORKER, status);
        int entrySize = status.Get_count(MPI::CHAR);
        char entry[entrySize];
        MPI::COMM_WORLD.Recv(entry, entrySize, MPI::CHAR, DATA_PROC, TO_WORKER);

        Car car = Car::from_json(string(entry, entry + entrySize));
        computeHash(car);
        if (car != Car()) {
            string serialized = car.to_json();
            entrySize = serialized.size();
            const char *entry = serialized.c_str();
            MPI::COMM_WORLD.Send(entry, entrySize, MPI::CHAR, RESULT_PROC, TO_RES);
        }
    }
}

void computeHash(Car &car) {
    SHA1 sha;
    sha.update(car.name + to_string(car.year) + to_string(car.mileage));
    car.hash = sha.final();

    if (FILTER_FROM_YEAR <= car.year) {
        return;
    }
    car = Car();
}

void insertItem(Car *cars, int &bufSize, Car item) {
    int i = 0;
    for (i = bufSize++; i > 0 && cars[i - 1].hash.compare(item.hash) > 0; i--) {
        cars[i] = cars[i - 1];
    }
    cars[i] = item;
}

void writeInitialData(vector<Car> cars) {
    const int NameWidth = 25;
    const int YearWidth = 5;
    const int MileageWidth = 10;
    const int HashWidth = 4;
    const char HorizontalSeparator = '-';
    const int HLength = NameWidth + YearWidth + MileageWidth + HashWidth + 11;
    const char *VerticalSeparator = " | ";

    ofstream output(OUTPUT_FILE);

    output << left << "Initial data" << endl;
    output << string(HLength, HorizontalSeparator) << endl;
    output << right << setw(NameWidth) << "Name" << VerticalSeparator
        << setw(YearWidth) << "Year" << VerticalSeparator << setw(MileageWidth)
        << "Mileage" << VerticalSeparator << setw(HashWidth) << "Hash"
        << VerticalSeparator << endl;
    output << string(HLength, HorizontalSeparator) << endl;
    for (int i = 0; i < cars.size(); i++) {
        Car car = cars[i];
        output << right << setw(NameWidth) << car.name << VerticalSeparator
            << setw(YearWidth) << car.year << VerticalSeparator << fixed
            << setprecision(2) << setw(MileageWidth) << car.mileage
            << VerticalSeparator << setw(HashWidth) << car.hash
            << VerticalSeparator << endl;
    }
    output << string(HLength, HorizontalSeparator) << endl;

    output.close();
}

void writeRes(vector<Car> cars) {
    const int NameWidth = 25;
    const int YearWidth = 5;
    const int MileageWidth = 10;
    const int HashWidth = 41;
    const char HorizontalSeparator = '-';
    const int HLength = NameWidth + YearWidth + MileageWidth + HashWidth + 11;
    const char *VerticalSeparator = " | ";

    ofstream output(OUTPUT_FILE, ios_base::app);

    output << left << "Results" << endl;
    output << string(HLength, HorizontalSeparator) << endl;
    output << right << setw(NameWidth) << "Name" << VerticalSeparator
        << setw(YearWidth) << "Year" << VerticalSeparator << setw(MileageWidth)
        << "Mileage" << VerticalSeparator << setw(HashWidth) << "Hash"
        << VerticalSeparator << endl;
    output << string(HLength, HorizontalSeparator) << endl;
    for (int i = 0; i < cars.size(); i++) {
        Car car = cars[i];
        output << right << setw(NameWidth) << car.name << VerticalSeparator
            << setw(YearWidth) << car.year << VerticalSeparator << fixed
            << setprecision(2) << setw(MileageWidth) << car.mileage
            << VerticalSeparator << setw(HashWidth) << car.hash
            << VerticalSeparator << endl;
    }
    output << string(HLength, HorizontalSeparator) << endl;

    output.close();
}
