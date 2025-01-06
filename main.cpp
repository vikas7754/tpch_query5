#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <thread>
#include <unordered_map>
#include <mutex>
#include <algorithm>
#include <chrono>
#include <functional>

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

// Batch parsing function
template<typename T>
void parseChunk(const vector<string> &lines, vector<T> &table, function<T(const string &)> parseFunc) {
    vector<T> localTable;
    for (const string &line : lines) {
        localTable.push_back(parseFunc(line));
    }

    // Append results to the shared table
    lock_guard<mutex> lock(mtx);
    table.insert(table.end(), localTable.begin(), localTable.end());
}

// Optimized table parsing with multithreading
template<typename T>
vector<T> parseTable(const string &filePath, function<T(const string &)> parseFunc, int numThreads = 4) {
    // Read the entire file into memory
    ifstream file(filePath);
    string data((istreambuf_iterator<char>(file)), istreambuf_iterator<char>());

    // Split data into lines
    vector<string> lines;
    size_t pos = 0, end;
    while ((end = data.find('\n', pos)) != string::npos) {
        lines.push_back(data.substr(pos, end - pos));
        pos = end + 1;
    }

    // Pre-allocate memory for the table
    vector<T> table;
    table.reserve(lines.size());

    // Divide work into chunks for multithreading
    size_t chunkSize = lines.size() / numThreads;
    vector<thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? lines.size() : (i + 1) * chunkSize;
        vector<string> chunk(lines.begin() + start, lines.begin() + end);

        threads.emplace_back(parseChunk<T>, chunk, ref(table), parseFunc);
    }

    for (thread &t : threads) {
        t.join();
    }

    return table;
}

// Optimized parsing functions
Customer parseCustomer(const string &line) {
    size_t start = 0, end;
    Customer customer;

    // Parse custkey
    end = line.find('|', start);
    customer.custkey = stoi(line.substr(start, end - start));
    start = end + 1;

    // Skip name and address
    start = line.find('|', start) + 1;
    start = line.find('|', start) + 1;

    // Parse nationkey
    end = line.find('|', start);
    customer.nationkey = stoi(line.substr(start, end - start));

    return customer;
}

Order parseOrder(const string &line) {
    size_t start = 0, end;
    Order order;

    // Parse orderkey
    end = line.find('|', start);
    order.orderkey = stoi(line.substr(start, end - start));
    start = end + 1;

    // Parse custkey
    end = line.find('|', start);
    order.custkey = stoi(line.substr(start, end - start));
    start = end + 1;

    // Skip other fields
    start = line.find('|', start) + 1;
    start = line.find('|', start) + 1;

    // Parse orderdate
    end = line.find('|', start);
    order.orderdate = line.substr(start, end - start);

    return order;
}

LineItem parseLineItem(const string &line) {
    size_t start = 0, end;
    LineItem lineItem;

    // Parse orderkey
    end = line.find('|', start);
    lineItem.orderkey = stoi(line.substr(start, end - start));
    start = end + 1;

    // Skip other fields
    start = line.find('|', start) + 1;

    // Parse suppkey
    end = line.find('|', start);
    lineItem.suppkey = stoi(line.substr(start, end - start));
    start = end + 1;

    // Skip other fields
    start = line.find('|', start) + 1;
    start = line.find('|', start) + 1;

    // Parse extendedprice
    end = line.find('|', start);
    lineItem.extendedprice = stod(line.substr(start, end - start));
    start = end + 1;

    // Parse discount
    end = line.find('|', start);
    lineItem.discount = stod(line.substr(start, end - start));

    return lineItem;
}

Supplier parseSupplier(const string &line) {
    size_t start = 0, end;
    Supplier supplier;

    // Parse suppkey
    end = line.find('|', start);
    supplier.suppkey = stoi(line.substr(start, end - start));
    start = end + 1;

    // Skip other fields
    start = line.find('|', start) + 1;
    start = line.find('|', start) + 1;

    // Parse nationkey
    end = line.find('|', start);
    supplier.nationkey = stoi(line.substr(start, end - start));

    return supplier;
}

Nation parseNation(const string &line) {
    size_t start = 0, end;
    Nation nation;

    // Parse nationkey
    end = line.find('|', start);
    nation.nationkey = stoi(line.substr(start, end - start));
    start = end + 1;

    // Parse name
    end = line.find('|', start);
    nation.name = line.substr(start, end - start);
    start = end + 1;

    // Parse regionkey
    end = line.find('|', start);
    nation.regionkey = stoi(line.substr(start, end - start));

    return nation;
}

Region parseRegion(const string &line) {
    size_t start = 0, end;
    Region region;

    // Parse regionkey
    end = line.find('|', start);
    region.regionkey = stoi(line.substr(start, end - start));
    start = end + 1;

    // Parse name
    end = line.find('|', start);
    region.name = line.substr(start, end - start);

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

// Function to process and save results
void saveResults(const vector<pair<string, double>> &resultVec, const string &outputDir) {
    // Construct the output file path
    string outputFilePath = outputDir + "/result.tbl";

    // Open the file for writing (overwrite if exists)
    ofstream outFile(outputFilePath);
    if (!outFile) {
        cerr << "Error: Could not write to file: " << outputFilePath << endl;
        return;
    }

    // Write results to the file
    for (const auto &[nation, revenue] : resultVec) {
        outFile << nation << "|" << revenue << endl;
    }

    cout << "Results saved to: " << outputFilePath << endl;

    // Close the file
    outFile.close();
}

// Thread manager
void threadManager(const vector<Order> &orders, const vector<LineItem> &lineItems,
                   const vector<Supplier> &suppliers, const vector<Nation> &nations,
                   const vector<Region> &regions, const string &regionName,
                   const string &startDate, const string &endDate, int numThreads, const string &outputDir) {
    // Each thread maintains its own local results
    vector<unordered_map<string, double>> threadResults(numThreads);

    size_t chunkSize = orders.size() / numThreads;
    vector<thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? orders.size() : (i + 1) * chunkSize;

        vector<Order> orderChunk(orders.begin() + start, orders.begin() + end);

        // Use thread-local results for each thread
        threads.emplace_back([&, i, orderChunk]() {
            unordered_map<string, double> localResults;
            processQuery(orderChunk, lineItems, suppliers, nations, regions, regionName, startDate, endDate, localResults);
            threadResults[i] = std::move(localResults); // Move results to threadResults[i]
        });
    }

    for (auto &t : threads) {
        t.join();
    }

    // Merge thread-local results into a global results map
    unordered_map<string, double> mergedResults;
    for (const auto &localResults : threadResults) {
        for (const auto &[key, value] : localResults) {
            mergedResults[key] += value; // Merge local results into global map
        }
    }

    // Sort results deterministically by key
    vector<pair<string, double>> resultVec(mergedResults.begin(), mergedResults.end());
    sort(resultVec.begin(), resultVec.end(), [](const auto &a, const auto &b) {
        return a.first < b.first; // Sort by nation name for deterministic order
    });

    // Output results
    for (const auto &[nation, revenue] : resultVec) {
        cout << nation << ": " << revenue << endl;
    }

    // Save results to file
    saveResults(resultVec, outputDir);
}

// Main function
int main(int argc, char *argv[]) {
    if (argc < 7) {
        cerr << "Usage: ./tpch_query5 <data_path> <region_name> <start_date> <end_date> <num_threads> <result_dir>\n";
        return 1;
    }

    string dataPath = argv[1];
    string regionName = argv[2];
    string startDate = argv[3];
    string endDate = argv[4];
    int numThreads = stoi(argv[5]);
    string outputDir = argv[6];

    cout << "Processing data..." << endl;
    auto start = high_resolution_clock::now();

    // Parse tables
    auto customers = parseTable<Customer>(dataPath + "/customer.tbl", parseCustomer);
    auto orders = parseTable<Order>(dataPath + "/orders.tbl", parseOrder);
    auto lineItems = parseTable<LineItem>(dataPath + "/lineitem.tbl", parseLineItem);
    auto suppliers = parseTable<Supplier>(dataPath + "/supplier.tbl", parseSupplier);
    auto nations = parseTable<Nation>(dataPath + "/nation.tbl", parseNation);
    auto regions = parseTable<Region>(dataPath + "/region.tbl", parseRegion);

    // Run query
    threadManager(orders, lineItems, suppliers, nations, regions, regionName, startDate, endDate, numThreads, outputDir);

    auto stop = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(stop - start);

    cout << fixed << setprecision(2);
    cout << "Time taken: " << duration.count() / 1000.0 << " s" << endl;
    
    return 0;
}