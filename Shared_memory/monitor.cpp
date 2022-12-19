#include <condition_variable>
#include <thread>
// Additional libraries used:
// https://github.com/nlohmann/json
#include <nlohmann/json.hpp>
// https://github.com/vog/sha1
#include "sha1-master/sha1.hpp"

using namespace std;
using json = nlohmann::json;

const string INPUT_FILE = "input.json";
const string OUTPUT_FILE = "output.txt";

const int THREAD_NUM = 2;
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

class Monitor {
public:
	Monitor(int _capacity) {
		arr = new Car[_capacity];
		count = 0;
		capacity = _capacity;
		canBeFilled = 1;
	}

	~Monitor() { delete[] arr; }

	bool isEmpty() { return (count == 0); }

	bool isFull() { return (count == capacity); }

	void addItem(Car newCar) {
		unique_lock<mutex> lock{ mtx };
		condVar.wait(lock, [this]() { return !isFull(); });

		arr[count++] = newCar;

		condVar.notify_all();
	}

	void addItemSorted(Car newCar) {
		unique_lock<mutex> lock{ mtx };

		insertItem(newCar);

		condVar.notify_all();
	}

	Car removeItem() {
		unique_lock<mutex> lock{ mtx };

		condVar.wait(lock, [this]() { return (isFull() || !canBeFilled); });

		if (count < 1) { return Car(); }

		Car car;
		car = arr[--count];

		condVar.notify_all();

		return car;
	}

	Car get(int index) {
		return arr[index];
	}

	void dataAddingEnded() {
		canBeFilled = 0;
		condVar.notify_all();
	}

	bool isDataGettingAdded() {
		return canBeFilled;
	}

	int size() {
		return count;
	}

private:
	Car* arr;
	int count;
	int capacity;

	bool canBeFilled;

	mutex mtx;
	condition_variable condVar;

	void insertItem(Car item) {
		int i;
		for (i = ++count - 1; i > 0 && arr[i - 1].hash.compare(item.hash) > 0; i--) {
			arr[i] = arr[i - 1];
		}
		arr[i] = item;
	}
};

vector<Car> readData();
void task(Monitor* dataMon, Monitor* resMon);
void writeInitialData(vector<Car> cars);
void writeRes(Monitor* resMon);

int main() {
	vector<Car> cars = readData();

	vector<thread> threads(THREAD_NUM);

	Monitor* dataMon = new Monitor(cars.size() / 2);
	Monitor* resMon = new Monitor(cars.size());

	for (int i = 0; i < THREAD_NUM; i++) {
		threads[i] = thread(task, dataMon, resMon);
	}

	for (Car c : cars) {
		dataMon->addItem(c);
	}

	dataMon->dataAddingEnded();

	for_each(threads.begin(), threads.end(), mem_fn(&thread::join));

	writeInitialData(cars);
	writeRes(resMon);

	delete dataMon;
	delete resMon;

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

void task(Monitor* dataMon, Monitor* resMon) {
	while (dataMon->isDataGettingAdded() || dataMon->size() > 0) {
		Car car = dataMon->removeItem();

		if (car == Car()) { break; }

		SHA1 sha;
		sha.update(car.name + to_string(car.year) + to_string(car.mileage));
		car.hash = sha.final();

		if (FILTER_FROM_YEAR <= car.year) {
			resMon->addItemSorted(car);
		}
	}
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
			<< car.mileage << VerticalSeparator << setw(HashWidth) << car.hash
			<< VerticalSeparator << endl;
	}
	output << string(HLength, HorizontalSeparator) << endl;

	output.close();
}

void writeRes(Monitor* resMon) {
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
	for (int i = 0; i < resMon->size(); i++)
	{
		Car car = resMon->get(i);
		output << right << setw(NameWidth) << car.name << VerticalSeparator << setw(YearWidth)
			<< car.year << VerticalSeparator << fixed << setprecision(2) << setw(MileageWidth)
			<< car.mileage << VerticalSeparator << setw(HashWidth) << car.hash
			<< VerticalSeparator << endl;
	}
	output << string(HLength, HorizontalSeparator) << endl;

	output.close();
}
