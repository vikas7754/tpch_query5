#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <thread>
#include <unordered_map>
#include <mutex>
#include <algorithm>
#include <chrono>

using namespace std;
using namespace std::chrono;

// Mutex for thread-safe access to shared resources
mutex mtx;

// Structures for tables
struct Customer {
    int custkey, nationkey;
};

struct Order {
    int orderkey, custkey;
    string orderdate;
};

struct LineItem {
    int orderkey, suppkey;
    double extendedprice, discount;
};

struct Supplier {
    int suppkey, nationkey;
};

struct Nation {
    int nationkey, regionkey;
    string name;
};

struct Region {
    int regionkey;
    string name;
};

// Function to parse a table
template<typename T>
vector<T> parseTable(const string &filePath, function<T(const string &)> parseFunc) {
    vector<T> table;
    ifstream file(filePath);
    string line;

    while (getline(file, line)) {
        table.push_back(parseFunc(line));
    }

    return table;
}

// Parse functions for each table
Customer parseCustomer(const string &line) {
    stringstream ss(line);
    string token;
    Customer customer;

    // Parse custkey
    getline(ss, token, '|');
    customer.custkey = stoi(token);

    getline(ss, token, '|'); // Skip name
    getline(ss, token, '|'); // Skip address

    // Parse nationkey
    getline(ss, token, '|');
    customer.nationkey = stoi(token);

    return customer;
}

Order parseOrder(const string &line) {
    stringstream ss(line);
    string token;
    Order order;

    getline(ss, token, '|');
    order.orderkey = stoi(token);

    getline(ss, token, '|');
    order.custkey = stoi(token);

    getline(ss, token, '|');
    getline(ss, token, '|');

    getline(ss, token, '|');
    order.orderdate = token;

    return order;
}

LineItem parseLineItem(const string &line) {
    stringstream ss(line);
    string token;
    LineItem lineItem;

    getline(ss, token, '|');
    lineItem.orderkey = stoi(token);

    getline(ss, token, '|');

    getline(ss, token, '|');
    lineItem.suppkey = stoi(token);

    getline(ss, token, '|');
    getline(ss, token, '|');

    getline(ss, token, '|');
    lineItem.extendedprice = stod(token);

    getline(ss, token, '|');
    lineItem.discount = stod(token);

    return lineItem;
}

Supplier parseSupplier(const string &line) {
    stringstream ss(line);
    string token;
    Supplier supplier;

    getline(ss, token, '|');
    supplier.suppkey = stoi(token);

    getline(ss, token, '|');
    getline(ss, token, '|');

    getline(ss, token, '|');
    supplier.nationkey = stoi(token);

    return supplier;
}

Nation parseNation(const string &line) {
    stringstream ss(line);
    string token;
    Nation nation;

    getline(ss, token, '|');
    nation.nationkey = stoi(token);

    getline(ss, token, '|');
    nation.name = token;

    getline(ss, token, '|');
    nation.regionkey = stoi(token);

    return nation;
}

Region parseRegion(const string &line) {
    stringstream ss(line);
    string token;
    Region region;

    getline(ss, token, '|');
    region.regionkey = stoi(token);

    getline(ss, token, '|');
    region.name = token;

    return region;
}

// Processing function
void processQuery(const vector<Order> &orders, const vector<LineItem> &lineItems,
                  const vector<Supplier> &suppliers, const vector<Nation> &nations,
                  const vector<Region> &regions, const string &regionName,
                  const string &startDate, const string &endDate,
                  unordered_map<string, double> &results) {
    // Filter regions
    unordered_map<int, string> regionNationMap;
    for (const auto &region : regions) {
        if (region.name == regionName) {
            for (const auto &nation : nations) {
                if (nation.regionkey == region.regionkey) {
                    regionNationMap[nation.nationkey] = nation.name;
                }
            }
        }
    }

    // Process orders in the given date range
    for (const auto &order : orders) {
        if (order.orderdate >= startDate && order.orderdate < endDate) {
            for (const auto &lineItem : lineItems) {
                if (lineItem.orderkey == order.orderkey) {
                    for (const auto &supplier : suppliers) {
                        if (supplier.suppkey == lineItem.suppkey && regionNationMap.count(supplier.nationkey)) {
                            double revenue = lineItem.extendedprice * (1 - lineItem.discount);

                            lock_guard<mutex> lock(mtx);
                            results[regionNationMap[supplier.nationkey]] += revenue;
                        }
                    }
                }
            }
        }
    }
}

// Thread manager
void threadManager(const vector<Order> &orders, const vector<LineItem> &lineItems,
                   const vector<Supplier> &suppliers, const vector<Nation> &nations,
                   const vector<Region> &regions, const string &regionName,
                   const string &startDate, const string &endDate, int numThreads) {
    unordered_map<string, double> results;

    size_t chunkSize = orders.size() / numThreads;
    vector<thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? orders.size() : (i + 1) * chunkSize;

        vector<Order> orderChunk(orders.begin() + start, orders.begin() + end);
        threads.emplace_back(processQuery, cref(orderChunk), cref(lineItems),
                             cref(suppliers), cref(nations), cref(regions),
                             cref(regionName), cref(startDate), cref(endDate),
                             ref(results));
    }

    for (auto &t : threads) {
        t.join();
    }

    // Display results
    vector<pair<string, double>> resultVec(results.begin(), results.end());
    sort(resultVec.begin(), resultVec.end(), [](auto &a, auto &b) { return a.second > b.second; });

    for (const auto &[nation, revenue] : resultVec) {
        cout << nation << ": " << revenue << endl;
    }
}

// Main function
int main(int argc, char *argv[]) {
    if (argc < 6) {
        cerr << "Usage: ./tpch_query5 <data_path> <region_name> <start_date> <end_date> <num_threads>\n";
        return 1;
    }

    cout << "Processing data..." << endl;
    auto start = high_resolution_clock::now();

    string dataPath = argv[1];
    string regionName = argv[2];
    string startDate = argv[3];
    string endDate = argv[4];
    int numThreads = stoi(argv[5]);

    // Parse tables
    auto customers = parseTable<Customer>(dataPath + "/customer.tbl", parseCustomer);
    auto orders = parseTable<Order>(dataPath + "/orders.tbl", parseOrder);
    auto lineItems = parseTable<LineItem>(dataPath + "/lineitem.tbl", parseLineItem);
    auto suppliers = parseTable<Supplier>(dataPath + "/supplier.tbl", parseSupplier);
    auto nations = parseTable<Nation>(dataPath + "/nation.tbl", parseNation);
    auto regions = parseTable<Region>(dataPath + "/region.tbl", parseRegion);

    // Run query
    threadManager(orders, lineItems, suppliers, nations, regions, regionName, startDate, endDate, numThreads);

    auto stop = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(stop - start);

    cout << fixed << setprecision(2);
    cout << "Time taken: " << duration.count() / 1000.0 << " s" << endl;
    
    return 0;
}