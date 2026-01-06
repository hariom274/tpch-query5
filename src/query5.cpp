#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <cstring>
#include <iomanip>
#include <unordered_map>
#include <unordered_set>



std::mutex results_mutex;

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--r_name") == 0 && i + 1 < argc) {
            r_name = argv[++i];
        } else if (strcmp(argv[i], "--start_date") == 0 && i + 1 < argc) {
            start_date = argv[++i];
        } else if (strcmp(argv[i], "--end_date") == 0 && i + 1 < argc) {
            end_date = argv[++i];
        } else if (strcmp(argv[i], "--threads") == 0 && i + 1 < argc) {
            num_threads = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--table_path") == 0 && i + 1 < argc) {
            table_path = argv[++i];
        } else if (strcmp(argv[i], "--result_path") == 0 && i + 1 < argc) {
            result_path = argv[++i];
        }
    }
    
    // Validate required parameters
    if (r_name.empty() || start_date.empty() || end_date.empty() || 
        num_threads <= 0 || table_path.empty() || result_path.empty()) {
        std::cerr << "Usage: " << argv[0] 
                  << " --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 "
                  << "--threads 4 --table_path /path/to/tables --result_path /path/to/results" << std::endl;
        return false;
    }
    
    return true;
}

// Helper function to split string by delimiter
std::vector<std::string> split(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::stringstream ss(str);
    std::string token;
    while (std::getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// Helper function to read a .tbl file
bool readTable(const std::string& filepath, std::vector<std::map<std::string, std::string>>& data, const std::vector<std::string>& columns) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        return false;
    }
    
    std::string line;
    while (std::getline(file, line)) {
        auto values = split(line, '|');
        if (values.size() >= columns.size()) {
            std::map<std::string, std::string> row;
            for (size_t i = 0; i < columns.size(); i++) {
                row[columns[i]] = values[i];
            }
            data.push_back(row);
        }
    }
    
    file.close();
    std::cout << "Loaded " << data.size() << " rows from " << filepath << std::endl;
    return true;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, 
                  std::vector<std::map<std::string, std::string>>& customer_data, 
                  std::vector<std::map<std::string, std::string>>& orders_data, 
                  std::vector<std::map<std::string, std::string>>& lineitem_data, 
                  std::vector<std::map<std::string, std::string>>& supplier_data, 
                  std::vector<std::map<std::string, std::string>>& nation_data, 
                  std::vector<std::map<std::string, std::string>>& region_data) {
    
    std::string base_path = table_path;
    if (base_path.back() != '/') base_path += '/';
    
    // Define columns for each table
    std::vector<std::string> customer_cols = {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"};
    std::vector<std::string> orders_cols = {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"};
    std::vector<std::string> lineitem_cols = {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"};
    std::vector<std::string> supplier_cols = {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"};
    std::vector<std::string> nation_cols = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
    std::vector<std::string> region_cols = {"r_regionkey", "r_name", "r_comment"};
    
    // Read all tables
    if (!readTable(base_path + "customer.tbl", customer_data, customer_cols)) return false;
    if (!readTable(base_path + "orders.tbl", orders_data, orders_cols)) return false;
    if (!readTable(base_path + "lineitem.tbl", lineitem_data, lineitem_cols)) return false;
    if (!readTable(base_path + "supplier.tbl", supplier_data, supplier_cols)) return false;
    if (!readTable(base_path + "nation.tbl", nation_data, nation_cols)) return false;
    if (!readTable(base_path + "region.tbl", region_data, region_cols)) return false;
    
    return true;
}
// Global hash maps (built once, shared by all threads)
std::unordered_map<std::string, std::string> g_orders_map;
std::unordered_map<std::string, std::string> g_customer_nation;
std::unordered_map<std::string, std::string> g_supplier_nation;
std::unordered_map<std::string, std::string> g_nation_name;
std::unordered_set<std::string> g_valid_nations;

// Simplified worker function
void processChunk(int start, int end, 
                  const std::vector<std::map<std::string, std::string>>& lineitem_data,
                  std::map<std::string, double>& results) {
    
    std::map<std::string, double> local_results;
    
    for (int i = start; i < end && i < lineitem_data.size(); i++) {
        const auto& lineitem = lineitem_data[i];
        std::string l_orderkey = lineitem.at("l_orderkey");
        std::string l_suppkey = lineitem.at("l_suppkey");
        
        // Check if order exists and is valid
        auto order_it = g_orders_map.find(l_orderkey);
        auto supp_it = g_supplier_nation.find(l_suppkey);
        
        if (order_it != g_orders_map.end() && supp_it != g_supplier_nation.end()) {
            std::string o_custkey = order_it->second;
            std::string s_nationkey = supp_it->second;
            
            auto cust_it = g_customer_nation.find(o_custkey);
            if (cust_it != g_customer_nation.end()) {
                std::string c_nationkey = cust_it->second;
                
                // Check if customer and supplier are from same nation
                if (c_nationkey == s_nationkey && g_valid_nations.count(c_nationkey) > 0) {
                    double l_extendedprice = std::stod(lineitem.at("l_extendedprice"));
                    double l_discount = std::stod(lineitem.at("l_discount"));
                    double revenue = l_extendedprice * (1.0 - l_discount);
                    
                    std::string n_name = g_nation_name[c_nationkey];
                    local_results[n_name] += revenue;
                }
            }
        }
    }
    
    // Merge local results into global results
    std::lock_guard<std::mutex> lock(results_mutex);
    for (const auto& pair : local_results) {
        results[pair.first] += pair.second;
    }
}


bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, 
                   int num_threads,
                   const std::vector<std::map<std::string, std::string>>& customer_data, 
                   const std::vector<std::map<std::string, std::string>>& orders_data, 
                   const std::vector<std::map<std::string, std::string>>& lineitem_data, 
                   const std::vector<std::map<std::string, std::string>>& supplier_data, 
                   const std::vector<std::map<std::string, std::string>>& nation_data, 
                   const std::vector<std::map<std::string, std::string>>& region_data, 
                   std::map<std::string, double>& results) {
    
    std::cout << "Building lookup tables..." << std::endl;
    
    // Build region filter
    std::unordered_set<std::string> asia_regions;
    for (const auto& region : region_data) {
        if (region.at("r_name") == r_name) {
            asia_regions.insert(region.at("r_regionkey"));
        }
    }
    
    // Build nation maps (only for ASIA region)
    for (const auto& nation : nation_data) {
        std::string n_regionkey = nation.at("n_regionkey");
        if (asia_regions.count(n_regionkey) > 0) {
            std::string n_nationkey = nation.at("n_nationkey");
            g_nation_name[n_nationkey] = nation.at("n_name");
            g_valid_nations.insert(n_nationkey);
        }
    }
    
    std::cout << "Processing customers..." << std::endl;
    // Build customer map (only for nations in ASIA)
    for (const auto& customer : customer_data) {
        std::string c_nationkey = customer.at("c_nationkey");
        if (g_valid_nations.count(c_nationkey) > 0) {
            g_customer_nation[customer.at("c_custkey")] = c_nationkey;
        }
    }
    
    std::cout << "Processing suppliers..." << std::endl;
    // Build supplier map (only for nations in ASIA)
    for (const auto& supplier : supplier_data) {
        std::string s_nationkey = supplier.at("s_nationkey");
        if (g_valid_nations.count(s_nationkey) > 0) {
            g_supplier_nation[supplier.at("s_suppkey")] = s_nationkey;
        }
    }
    
    std::cout << "Processing orders..." << std::endl;
    // Build orders map (only for valid date range and customers)
    for (const auto& order : orders_data) {
        std::string o_orderdate = order.at("o_orderdate");
        if (o_orderdate >= start_date && o_orderdate < end_date) {
            std::string o_custkey = order.at("o_custkey");
            if (g_customer_nation.count(o_custkey) > 0) {
                g_orders_map[order.at("o_orderkey")] = o_custkey;
            }
        }
    }
    
    std::cout << "Starting query execution with " << num_threads << " thread(s)..." << std::endl;
    
    int total_rows = lineitem_data.size();
    int chunk_size = (total_rows + num_threads - 1) / num_threads;
    
    std::vector<std::thread> threads;
    
    for (int t = 0; t < num_threads; t++) {
        int start = t * chunk_size;
        int end = std::min(start + chunk_size, total_rows);
        
        if (start < total_rows) {
            threads.emplace_back(processChunk, start, end, 
                               std::ref(lineitem_data),
                               std::ref(results));
        }
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    std::cout << "Query execution completed." << std::endl;
    return true;
}



// Function to output results to the specified path
// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    // Convert map to vector for sorting
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());
    
    // Sort by revenue descending - using explicit type instead of auto
    std::sort(sorted_results.begin(), sorted_results.end(), 
              [](const std::pair<std::string, double>& a, const std::pair<std::string, double>& b) { 
                  return a.second > b.second; 
              });
    
    // Output to file
    std::ofstream outfile(result_path);
    if (!outfile.is_open()) {
        std::cerr << "Failed to open output file: " << result_path << std::endl;
        return false;
    }
    
    outfile << "n_name|revenue" << std::endl;
    for (const auto& pair : sorted_results) {
        outfile << pair.first << "|" << std::fixed << std::setprecision(2) << pair.second << std::endl;
        std::cout << pair.first << "|" << std::fixed << std::setprecision(2) << pair.second << std::endl;
    }
    
    outfile.close();
    return true;
}

