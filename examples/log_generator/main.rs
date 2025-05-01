use chrono::{DateTime, Datelike, Duration, Local, TimeZone, Timelike};
use rand::prelude::*;
use rand_distr::weighted::WeightedIndex;
use rand_distr::Distribution;
use rand_distr::{Gamma, LogNormal};
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use uuid::Uuid;

// --- Enhancements for Realistic Load Simulation ---
// 1. Diurnal traffic pattern modeling (hourly traffic curve)
// 2. Bot/crawler traffic simulation
// 3. Enhanced session and user journey modeling
// 4. Error/anomaly burst injection utilities

/// Represents the type of actor generating the request.
#[derive(Clone, Debug)]
enum ActorType {
    Human,
    Bot,
}

/// Helper for diurnal traffic intensity (returns multiplier for given hour)
fn diurnal_traffic_multiplier(hour: u32, is_weekend: bool) -> f64 {
    if is_weekend {
        // Lower, flatter traffic on weekends
        match hour {
            10..=20 => 0.4,
            _ => 0.1,
        }
    } else {
        // Workday: ramp up, peak, lunch dip, afternoon peak, evening drop
        match hour {
            7..=8 => 0.3,
            9..=11 => 0.7,
            12 => 0.5,
            13..=16 => 1.0,
            17..=18 => 0.6,
            19..=21 => 0.3,
            _ => 0.1,
        }
    }
}

/// Returns a random bot user agent string.
fn random_bot_user_agent(rng: &mut StdRng) -> &'static str {
    let bots = [
        "Googlebot/2.1 (+http://www.google.com/bot.html)",
        "Bingbot/2.0 (+http://www.bing.com/bingbot.htm)",
        "DuckDuckBot/1.0; (+http://duckduckgo.com/duckduckbot.html)",
        "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
        "Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)",
    ];
    bots[rng.gen_range(0..bots.len())]
}
#[derive(Serialize, Debug, Clone)]
struct LogEntry {
    timestamp: String,
    request_id: String,
    service_name: String,
    endpoint: String,
    method: String,
    status_code: u16,
    response_time_ms: u32,
    user_id: String,
    client_ip: String,
    user_agent: String,
    request_size_bytes: u32,
    response_size_bytes: u32,
    content_type: String,
    is_error: bool,
    error_type: Option<String>,
    geo_region: String,
    has_external_call: bool,
    external_service: Option<String>,
    external_endpoint: Option<String>,
    external_call_time_ms: Option<u32>,
    external_call_status: Option<u16>,
    db_query: Option<String>,
    db_name: Option<String>,
    db_execution_time_ms: Option<u32>,
    cpu_utilization: f32,
    memory_utilization: f32,
    disk_io: f32,
    network_io: f32,
}

fn main() -> std::io::Result<()> {
    // Set up RNG with seed
    let mut rng = StdRng::seed_from_u64(42);

    // Base number of logs
    let n_logs = 1_000_000;
    let year = 2024;
    let month = 4;
    // Time range for logs (7 days)
    let start_date = Local.ymd(year, month, 1).and_hms(0, 0, 0);
    let end_date = start_date + Duration::days(7);

    // Services with weights
    let services = [
        "api-gateway",
        "auth-service",
        "user-service",
        "product-service",
        "payment-service",
        "search-service",
    ];
    let service_weights = vec![0.3, 0.15, 0.2, 0.15, 0.1, 0.1];
    let service_dist = WeightedIndex::new(&service_weights).unwrap();

    // Define endpoints by service
    let mut endpoints = HashMap::new();
    endpoints.insert("api-gateway", vec!["/v1/gateway", "/v1/proxy", "/health"]);
    endpoints.insert(
        "auth-service",
        vec![
            "/v1/login",
            "/v1/register",
            "/v1/token",
            "/v1/oauth",
            "/v1/logout",
            "/health",
        ],
    );
    endpoints.insert(
        "user-service",
        vec![
            "/v1/users",
            "/v1/users/{id}",
            "/v1/profiles",
            "/v1/preferences",
            "/health",
        ],
    );
    endpoints.insert(
        "product-service",
        vec![
            "/v1/products",
            "/v1/products/{id}",
            "/v1/categories",
            "/v1/inventory",
            "/health",
        ],
    );
    endpoints.insert(
        "payment-service",
        vec!["/v1/payments", "/v1/transactions", "/v1/refunds", "/health"],
    );
    endpoints.insert(
        "search-service",
        vec!["/v1/search", "/v1/suggest", "/v1/trending", "/health"],
    );

    // HTTP methods with weights
    let methods = ["GET", "POST", "PUT", "DELETE"];
    let method_weights = vec![0.7, 0.15, 0.1, 0.05];
    let method_dist = WeightedIndex::new(&method_weights).unwrap();

    // Generate user IDs
    let n_users = 500;
    let user_ids: Vec<String> = (1..=n_users).map(|i| format!("user_{}", i)).collect();

    // Mark some users as active (higher weight)
    let mut active_users_indices = Vec::new();
    for _ in 0..50 {
        let idx = rng.gen_range(0..user_ids.len());
        active_users_indices.push(idx);
    }

    let mut user_weights = vec![1.0; user_ids.len()];
    for &idx in &active_users_indices {
        user_weights[idx] = 5.0;
    }

    // Normalize weights
    let sum_weights: f64 = user_weights.iter().sum();
    user_weights = user_weights.iter().map(|&w| w / sum_weights).collect();
    let user_dist = WeightedIndex::new(&user_weights).unwrap();

    // Regional IP distribution
    let regions = vec![
        "us-east",
        "us-west",
        "eu-west",
        "eu-central",
        "asia-east",
        "asia-south",
    ];
    let mut region_ips = HashMap::new();

    for &region in &regions {
        let mut ips = Vec::new();
        for _ in 0..50 {
            let ip = format!(
                "{}-{}.{}.{}",
                &region[0..2],
                rng.gen_range(1..256),
                rng.gen_range(0..256),
                rng.gen_range(1..255)
            );
            ips.push(ip);
        }
        region_ips.insert(region, ips);
    }

    // External dependencies
    let external_services = vec![
        "payment-gateway",
        "email-service",
        "sms-service",
        "mapping-service",
        "analytics-service",
    ];

    let mut external_endpoints = HashMap::new();
    for &service in &external_services {
        external_endpoints.insert(service, vec!["/process", "/verify", "/send", "/track"]);
    }

    // Database services
    let db_names = ["users_db", "products_db", "transactions_db", "analytics_db"];

    // User agents
    let user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6) Mobile Safari/604.1",
        "Mozilla/5.0 (Linux; Android 11) Chrome/91.0.4472.120 Mobile",
    ];

    // Generate timestamps with diurnal and bot/human patterns
    let mut timestamp_actors =
        generate_realistic_timestamps(n_logs, start_date, end_date, 0.05, &mut rng);
    timestamp_actors.sort_by(|a, b| a.0.cmp(&b.0));

    // Generate all logs
    let mut logs = Vec::with_capacity(n_logs + 5000); // Add extra capacity for special patterns

    for (timestamp, actor_type) in timestamp_actors {
        let request_id = Uuid::new_v4().to_string();

        // Branch for bot vs human
        let (service_name, endpoint, method, user_id, user_agent, geo_region, client_ip) =
            match actor_type {
                ActorType::Bot => {
                    // Bots crawl a variety of endpoints, often GET, random user_id, bot user agent
                    let bot_services = ["api-gateway", "product-service", "search-service"];
                    let service_name =
                        bot_services[rng.gen_range(0..bot_services.len())].to_string();
                    let service_endpoints = endpoints.get(service_name.as_str()).unwrap();
                    let endpoint =
                        service_endpoints[rng.gen_range(0..service_endpoints.len())].to_string();
                    let method = "GET".to_string();
                    let user_id = "bot".to_string();
                    let user_agent = random_bot_user_agent(&mut rng).to_string();
                    let geo_region = regions[rng.gen_range(0..regions.len())].to_string();
                    let region_ip_list = region_ips.get(geo_region.as_str()).unwrap();
                    let client_ip = region_ip_list[rng.gen_range(0..region_ip_list.len())].clone();
                    (
                        service_name,
                        endpoint,
                        method,
                        user_id,
                        user_agent,
                        geo_region,
                        client_ip,
                    )
                }
                ActorType::Human => {
                    let service_idx = service_dist.sample(&mut rng);
                    let service_name = services[service_idx].to_string();
                    let service_endpoints = endpoints.get(service_name.as_str()).unwrap();
                    let endpoint_idx = rng.gen_range(0..service_endpoints.len());
                    let mut endpoint = service_endpoints[endpoint_idx].to_string();
                    // Replace {id} with a random number if present
                    if endpoint.contains("{id}") {
                        let id = rng.gen_range(1..1001).to_string();
                        endpoint = endpoint.replace("{id}", &id);
                    }
                    let method_idx = method_dist.sample(&mut rng);
                    let method = methods[method_idx].to_string();
                    let user_idx = user_dist.sample(&mut rng);
                    let user_id = user_ids[user_idx].clone();
                    let region_idx = rng.gen_range(0..regions.len());
                    let geo_region = regions[region_idx].to_string();
                    let region_ip_list = region_ips.get(geo_region.as_str()).unwrap();
                    let ip_idx = rng.gen_range(0..region_ip_list.len());
                    let client_ip = region_ip_list[ip_idx].clone();
                    let ua_idx = rng.gen_range(0..user_agents.len());
                    let user_agent = user_agents[ua_idx].to_string();
                    (
                        service_name,
                        endpoint,
                        method,
                        user_id,
                        user_agent,
                        geo_region,
                        client_ip,
                    )
                }
            };
        let mut endpoint = endpoint.clone();
        // Replace {id} with a random number if present
        if endpoint.contains("{id}") {
            let id = rng.gen_range(1..1001).to_string();
            endpoint = endpoint.replace("{id}", &id);
        }

        let method_idx = method_dist.sample(&mut rng);
        let method = methods[method_idx].to_string();

        // Error distribution (5% chance of error)
        let is_error = rng.gen::<f64>() < 0.05;
        let status_code = if is_error {
            *[400, 401, 403, 404, 429, 500, 502, 503]
                .choose(&mut rng)
                .unwrap()
        } else {
            *[200, 201, 204, 301, 302].choose(&mut rng).unwrap()
        };

        // Response time distribution based on endpoint
        let mut base_time = 50.0; // Default 50ms
        if endpoint.contains("/search") {
            base_time = 300.0; // Search is slower
        } else if endpoint.contains("/payments") {
            base_time = 250.0; // Payments have higher latency
        } else if endpoint.contains("/login") {
            base_time = 100.0;
        }

        // Generate response time with gamma distribution
        let gamma = Gamma::new(2.0, base_time / 2.0).unwrap();
        let mut response_time_ms = gamma.sample(&mut rng) as u32;

        // Add outliers (1% chance)
        if rng.gen::<f64>() < 0.01 {
            response_time_ms *= rng.gen_range(5..11);
        }

        // Select user
        let user_idx = user_dist.sample(&mut rng);
        let user_id = user_ids[user_idx].clone();

        // Select region and IP
        let region_idx = rng.gen_range(0..regions.len());
        let geo_region = regions[region_idx].to_string();
        let region_ip_list = region_ips.get(geo_region.as_str()).unwrap();
        let ip_idx = rng.gen_range(0..region_ip_list.len());
        let client_ip = region_ip_list[ip_idx].clone();

        // User agent
        let ua_idx = rng.gen_range(0..user_agents.len());
        let user_agent = user_agents[ua_idx].to_string();

        // Request/response sizes
        let lognormal_req = LogNormal::new(7.0, 1.0).unwrap();
        let lognormal_resp = LogNormal::new(8.0, 1.5).unwrap();
        let request_size_bytes = lognormal_req.sample(&mut rng) as u32;
        let response_size_bytes = lognormal_resp.sample(&mut rng) as u32;

        let content_types = ["application/json", "text/html", "application/xml"];
        let content_type = content_types[rng.gen_range(0..content_types.len())].to_string();

        // Error information
        let error_type = if is_error {
            if (400..500).contains(&status_code) {
                Some(
                    ["validation_error", "auth_error", "not_found", "rate_limit"]
                        .choose(&mut rng)
                        .unwrap()
                        .to_string(),
                )
            } else {
                Some(
                    ["server_error", "db_error", "timeout", "dependency_error"]
                        .choose(&mut rng)
                        .unwrap()
                        .to_string(),
                )
            }
        } else {
            None
        };

        // External service calls
        let has_external_call = (service_name == "payment-service"
            || service_name == "api-gateway")
            && rng.gen::<f64>() < 0.8;
        let external_service = if has_external_call {
            Some(external_services[rng.gen_range(0..external_services.len())].to_string())
        } else {
            None
        };

        let external_endpoint = if has_external_call {
            let service = external_service.as_ref().unwrap();
            let endpoints = external_endpoints.get(service.as_str()).unwrap();
            Some(endpoints[rng.gen_range(0..endpoints.len())].to_string())
        } else {
            None
        };

        let external_call_time_ms = if has_external_call {
            let gamma = Gamma::new(2.0, 30.0).unwrap();
            Some(gamma.sample(&mut rng) as u32)
        } else {
            None
        };

        let mut external_call_status = None;
        let mut status_code_copy = status_code; // For potential modifications
        let mut is_error_copy = is_error;
        let mut error_type_copy = error_type.clone();

        if has_external_call {
            external_call_status = if rng.gen::<f64>() < 0.95 {
                Some(200)
            } else {
                Some(*[400, 500, 503].choose(&mut rng).unwrap())
            };

            // External errors often cause main service errors
            if external_call_status.unwrap() >= 400 && rng.gen::<f64>() < 0.7 {
                status_code_copy = *[500, 502, 503].choose(&mut rng).unwrap();
                is_error_copy = true;
                error_type_copy = Some("dependency_error".to_string());
            }
        }

        // Database operations
        let has_db = ["user-service", "product-service", "payment-service"]
            .contains(&service_name.as_str())
            && rng.gen::<f64>() < 0.7;
        let db_name = if has_db {
            Some(db_names[rng.gen_range(0..db_names.len())].to_string())
        } else {
            None
        };

        let db_query = if has_db {
            let query_type = if method == "GET" {
                "SELECT"
            } else if method == "POST" {
                "INSERT"
            } else if method == "PUT" {
                "UPDATE"
            } else {
                "DELETE"
            };

            let table = db_name.as_ref().unwrap().split('_').next().unwrap();
            let field = *["*", "id", "name", "value"].choose(&mut rng).unwrap();

            Some(format!(
                "{} {} {} {}",
                query_type,
                field,
                if query_type != "INSERT" {
                    "FROM"
                } else {
                    "INTO"
                },
                table
            ))
        } else {
            None
        };

        let db_execution_time_ms = if has_db {
            let gamma = Gamma::new(2.0, 10.0).unwrap();
            let mut time = gamma.sample(&mut rng) as u32;

            // DB errors occasionally cause service errors
            if rng.gen::<f64>() < 0.03 {
                time *= rng.gen_range(5..16);
                if rng.gen::<f64>() < 0.6 {
                    status_code_copy = 500;
                    is_error_copy = true;
                    error_type_copy = Some("db_error".to_string());
                }
            }

            Some(time)
        } else {
            None
        };

        // Resource metrics
        let cpu_utilization = rng.gen_range(10.0..90.0);
        let memory_utilization = rng.gen_range(20.0..80.0);
        let disk_io = rng.gen_range(5.0..60.0);
        let network_io = rng.gen_range(10.0..200.0);

        // Create log entry
        let log_entry = match actor_type {
            ActorType::Bot => LogEntry {
                timestamp: timestamp.to_rfc3339(),
                request_id,
                service_name,
                endpoint,
                method,
                status_code: 200,
                response_time_ms: rng.gen_range(20..80),
                user_id,
                client_ip,
                user_agent,
                request_size_bytes: rng.gen_range(200..800),
                response_size_bytes: rng.gen_range(500..2000),
                content_type: "text/html".to_string(),
                is_error: false,
                error_type: None,
                geo_region,
                has_external_call: false,
                external_service: None,
                external_endpoint: None,
                external_call_time_ms: None,
                external_call_status: None,
                db_query: None,
                db_name: None,
                db_execution_time_ms: None,
                cpu_utilization: rng.gen_range(5.0..20.0),
                memory_utilization: rng.gen_range(5.0..20.0),
                disk_io: rng.gen_range(1.0..10.0),
                network_io: rng.gen_range(2.0..20.0),
            },
            ActorType::Human => LogEntry {
                timestamp: timestamp.to_rfc3339(),
                request_id,
                service_name,
                endpoint,
                method,
                status_code: status_code_copy,
                response_time_ms,
                user_id,
                client_ip,
                user_agent,
                request_size_bytes,
                response_size_bytes,
                content_type,
                is_error: is_error_copy,
                error_type: error_type_copy,
                geo_region,
                has_external_call,
                external_service,
                external_endpoint,
                external_call_time_ms,
                external_call_status,
                db_query,
                db_name,
                db_execution_time_ms,
                cpu_utilization,
                memory_utilization,
                disk_io,
                network_io,
            },
        };

        logs.push(log_entry);
    }

    // SPECIAL PATTERN 1: Add traffic spike for anomaly detection
    let spike_start = Local.ymd(year, month, 3).and_hms(10, 0, 0);

    for _ in 0..2000 {
        let seconds_offset = rng.gen_range(0..3600);
        let spike_time = spike_start + Duration::seconds(seconds_offset);

        // Pick a random existing log as a template
        let template_idx = rng.gen_range(0..logs.len());
        let mut template = logs[template_idx].clone();

        // Modify the template for the spike
        template.timestamp = spike_time.to_rfc3339();
        template.request_id = Uuid::new_v4().to_string();
        template.service_name = "api-gateway".to_string();

        // Higher error rate during spike (15%)
        if rng.gen::<f64>() < 0.15 {
            template.status_code = 503;
            template.is_error = true;
            template.error_type = Some("timeout".to_string());
            template.response_time_ms = rng.gen_range(1000..5001);
        }

        logs.push(template);
    }

    // SPECIAL PATTERN 2: Add user session patterns
    let mut session_user_indices = Vec::new();
    for _ in 0..10 {
        session_user_indices.push(rng.gen_range(0..user_ids.len()));
    }

    for &user_idx in &session_user_indices {
        let user = &user_ids[user_idx];

        for _ in 0..rng.gen_range(3..6) {
            // 3-5 sessions per user
            let day_offset = rng.gen_range(0..7);
            let hour_offset = rng.gen_range(8..21);
            let session_start =
                start_date + Duration::days(day_offset) + Duration::hours(hour_offset);
            let session_duration_mins = rng.gen_range(5..61);

            // Create 5-15 sequential requests in a session
            let n_requests = rng.gen_range(5..16);

            for i in 0..n_requests {
                let progress = (i as f64) / (n_requests as f64);
                let req_time = session_start
                    + Duration::minutes((session_duration_mins as f64 * progress) as i64);

                // Follow a typical user journey pattern
                let (endpoint, service, method) = if i == 0 {
                    (
                        "/v1/login".to_string(),
                        "auth-service".to_string(),
                        "POST".to_string(),
                    )
                } else if i == n_requests - 1 {
                    (
                        "/v1/logout".to_string(),
                        "auth-service".to_string(),
                        "POST".to_string(),
                    )
                } else {
                    let is_product = rng.gen_bool(0.5);
                    if is_product {
                        (
                            format!("/v1/products/{}", rng.gen_range(1..1001)),
                            "product-service".to_string(),
                            "GET".to_string(),
                        )
                    } else {
                        let queries = ["laptop", "phone", "tablet"];
                        let query = queries[rng.gen_range(0..queries.len())];
                        (
                            format!("/v1/search?q={}", query),
                            "search-service".to_string(),
                            "GET".to_string(),
                        )
                    }
                };

                let session_log = LogEntry {
                    timestamp: req_time.to_rfc3339(),
                    request_id: Uuid::new_v4().to_string(),
                    service_name: service,
                    endpoint,
                    method,
                    status_code: 200,
                    response_time_ms: rng.gen_range(30..201),
                    user_id: user.clone(),
                    client_ip: {
                        let all_ips: Vec<String> =
                            region_ips.values().flat_map(|v| v.clone()).collect();
                        all_ips[rng.gen_range(0..all_ips.len())].clone()
                    },
                    user_agent: user_agents[rng.gen_range(0..user_agents.len())].to_string(),
                    request_size_bytes: rng.gen_range(500..2001),
                    response_size_bytes: rng.gen_range(1000..10001),
                    content_type: "application/json".to_string(),
                    is_error: false,
                    error_type: None,
                    geo_region: regions[rng.gen_range(0..regions.len())].to_string(),
                    has_external_call: false,
                    external_service: None,
                    external_endpoint: None,
                    external_call_time_ms: None,
                    external_call_status: None,
                    db_query: None,
                    db_name: None,
                    db_execution_time_ms: None,
                    cpu_utilization: rng.gen_range(10.0..90.0),
                    memory_utilization: rng.gen_range(20.0..80.0),
                    disk_io: rng.gen_range(5.0..60.0),
                    network_io: rng.gen_range(10.0..200.0),
                };

                logs.push(session_log);
            }
        }
    }

    // Sort all logs by timestamp
    logs.sort_by(|a, b| {
        DateTime::parse_from_rfc3339(&a.timestamp)
            .unwrap()
            .cmp(&DateTime::parse_from_rfc3339(&b.timestamp).unwrap())
    });

    // Write logs to NDJSON file
    let file = File::create("request_logs.json")?;
    let mut writer = BufWriter::new(file);

    for log in &logs {
        let json = serde_json::to_string(&log)?;
        writeln!(writer, "{}", json)?;
    }

    println!("Generated {} log entries in request_logs.json", logs.len());

    Ok(())
}

// Helper function to generate timestamps with business hour patterns
fn generate_timestamps(
    n: usize,
    start_date: DateTime<Local>,
    end_date: DateTime<Local>,
    rng: &mut StdRng,
) -> Vec<DateTime<Local>> {
    let mut timestamps = Vec::with_capacity(n);
    let duration_days = (end_date - start_date).num_days();

    for _ in 0..n {
        let random_day = rng.gen_range(0..=duration_days);
        let mut random_date = start_date + Duration::days(random_day);

        // 70% of traffic during business hours (9 AM - 5 PM)
        let hour = if rng.gen::<f64>() < 0.7 {
            rng.gen_range(9..18)
        } else {
            rng.gen_range(0..24)
        };

        let minute = rng.gen_range(0..60);
        let second = rng.gen_range(0..60);

        random_date = random_date
            .with_hour(hour)
            .unwrap()
            .with_minute(minute)
            .unwrap()
            .with_second(second)
            .unwrap();

        timestamps.push(random_date);
    }

    timestamps
}
/// Generates timestamps with diurnal/weekday-aware traffic intensity.
/// Returns a vector of (timestamp, ActorType) for both human and bot traffic.
fn generate_realistic_timestamps(
    n: usize,
    start_date: DateTime<Local>,
    end_date: DateTime<Local>,
    bot_fraction: f64,
    rng: &mut StdRng,
) -> Vec<(DateTime<Local>, ActorType)> {
    let mut timestamps = Vec::with_capacity(n);
    let duration_days = (end_date - start_date).num_days();

    // Estimate total human and bot logs
    let n_bots = ((n as f64) * bot_fraction).round() as usize;
    let n_humans = n - n_bots;

    // Human traffic: distribute by diurnal multiplier
    for _ in 0..n_humans {
        let random_day = rng.gen_range(0..=duration_days);
        let mut random_date = start_date + Duration::days(random_day);

        let is_weekend = matches!(
            random_date.weekday(),
            chrono::Weekday::Sat | chrono::Weekday::Sun
        );

        // Weighted hour selection
        let mut hour_weights = vec![];
        for hour in 0..24 {
            hour_weights.push(diurnal_traffic_multiplier(hour, is_weekend));
        }
        let hour_dist = WeightedIndex::new(&hour_weights).unwrap();
        let hour = hour_dist.sample(rng) as u32;

        let minute = rng.gen_range(0..60);
        let second = rng.gen_range(0..60);

        random_date = random_date
            .with_hour(hour)
            .unwrap()
            .with_minute(minute)
            .unwrap()
            .with_second(second)
            .unwrap();

        timestamps.push((random_date, ActorType::Human));
    }

    // Bot/crawler traffic: flatter, but with possible bursts
    for _ in 0..n_bots {
        let random_day = rng.gen_range(0..=duration_days);
        let mut random_date = start_date + Duration::days(random_day);

        // Bots crawl at all hours, but sometimes burst at night
        let hour = if rng.gen::<f64>() < 0.2 {
            rng.gen_range(0..6) // Night burst
        } else {
            rng.gen_range(0..24)
        };

        let minute = rng.gen_range(0..60);
        let second = rng.gen_range(0..60);

        random_date = random_date
            .with_hour(hour)
            .unwrap()
            .with_minute(minute)
            .unwrap()
            .with_second(second)
            .unwrap();

        timestamps.push((random_date, ActorType::Bot));
    }

    timestamps
}
