import json
from kafka import KafkaConsumer
from datetime import datetime, timedelta
from allocator import allocate
from kafka_producer import kafka_producer


KAFKA_TOPIC = "mock_11_stream"
KAFKA_BROKER = 'localhost:9092'
ORDER_SIZE = 5000  
FEE = 0.003
REBATE = 0.002

#FEE AND REBATE USED FROM THE MODEL
#USING DIFEERENT PARAMETER VALUES, THESE VALUES ARE COMMONLY USED IN MODELS -> AS MENTIONED IN THE PAPER, WE WILL FIND
# THE BEST ONE FROM THESE 
PARAM_GRID = [
    {"lambda_over": 0.2, "lambda_under": 0.2, "theta_queue": 0.1},
    {"lambda_over": 0.4, "lambda_under": 0.6, "theta_queue": 0.3},
    {"lambda_over": 0.5, "lambda_under": 0.5, "theta_queue": 0.2},
]

def format_venues(snapshot):
    return [
        {
            "ask_px_00": data['ask_px_00'],
            "ask_sz_00": data['ask_sz_00'],
            "fee": FEE,
            "rebate": REBATE
        } for vid, data in snapshot.items()
    ]

def run_sor_simulation(snapshots, params):
    remaining_shares = ORDER_SIZE
    total_cash_spent = 0.0
    
    for _, snapshot in snapshots:
        if remaining_shares <= 0:
            break

        venues = format_venues(snapshot)
        if not venues:
            continue

        split, _ = allocate(
            remaining_shares, venues,
            lambda_over=params["lambda_over"],
            lambda_under=params["lambda_under"],
            theta_queue=params["theta_queue"]
        )

        if not split:
            continue
            
        executed_this_turn = 0
        cash_spent_this_turn = 0.0
        
        for i, venue in enumerate(venues):
            fill_qty = min(split[i], venue['ask_sz_00'])
            cost = fill_qty * (venue['ask_px_00'] + FEE)
            
            executed_this_turn += fill_qty
            cash_spent_this_turn += cost

        remaining_shares -= executed_this_turn
        total_cash_spent += cash_spent_this_turn

    executed_total = ORDER_SIZE - remaining_shares
    avg_fill_px = total_cash_spent / executed_total if executed_total > 0 else 0
    return total_cash_spent, avg_fill_px

def run_best_ask_simulation(snapshots):
    remaining_shares = ORDER_SIZE
    total_cash_spent = 0.0
    
    for _, snapshot in snapshots:
        if remaining_shares <= 0:
            break
        
        sorted_venues = sorted(snapshot.values(), key=lambda v: v['ask_px_00'])
        
        for venue in sorted_venues:
            if remaining_shares <= 0:
                break
            fill = min(remaining_shares, venue['ask_sz_00'])
            total_cash_spent += fill * (venue['ask_px_00'] + FEE)
            remaining_shares -= fill
            
    executed_total = ORDER_SIZE - remaining_shares
    avg_fill_px = total_cash_spent / executed_total if executed_total > 0 else 0
    return total_cash_spent, avg_fill_px

def run_vwap_simulation(snapshots):
    remaining_shares = ORDER_SIZE
    total_cash_spent = 0.0

    for _, snapshot in snapshots:
        if remaining_shares <= 0:
            break
        
        venues = list(snapshot.values())
        total_size = sum(v['ask_sz_00'] for v in venues)
        
        if total_size <= 0:
            continue

        for venue in venues:
            ratio = venue['ask_sz_00'] / total_size
            alloc = int(remaining_shares * ratio)
            fill = min(alloc, venue['ask_sz_00'])
            total_cash_spent += fill * (venue['ask_px_00'] + FEE)
        
        executed_this_turn = sum(min(int(remaining_shares * (v['ask_sz_00']/total_size)), v['ask_sz_00']) for v in venues)
        remaining_shares -= executed_this_turn

    executed_total = ORDER_SIZE - remaining_shares
    avg_fill_px = total_cash_spent / executed_total if executed_total > 0 else 0
    return total_cash_spent, avg_fill_px

def run_twap_simulation(snapshots, interval_seconds=60):
    if not snapshots:
        return 0, 0
    
    start_time = datetime.fromisoformat(snapshots[0][0].replace('Z', '+00:00'))
    end_time = datetime.fromisoformat(snapshots[-1][0].replace('Z', '+00:00'))
    duration = (end_time - start_time).total_seconds()
    num_intervals = max(1, int(duration / interval_seconds))
    shares_per_interval = ORDER_SIZE // num_intervals
    
    total_cash_spent = 0.0
    executed_total = 0

    for i in range(num_intervals):
        interval_start_time = start_time + timedelta(seconds=i * interval_seconds)
        interval_end_time = interval_start_time + timedelta(seconds=interval_seconds)
        
        try:
            target_snapshot = next(snap for ts, snap in snapshots if interval_start_time <= datetime.fromisoformat(ts.replace('Z', '+00:00')) < interval_end_time)
        except StopIteration:
            continue 

        remaining_in_chunk = shares_per_interval
        sorted_venues = sorted(target_snapshot.values(), key=lambda v: v['ask_px_00'])

        for venue in sorted_venues:
            if remaining_in_chunk <= 0:
                break
            fill = min(remaining_in_chunk, venue['ask_sz_00'])
            total_cash_spent += fill * (venue['ask_px_00'] + FEE)
            remaining_in_chunk -= fill
        
        executed_total += (shares_per_interval - remaining_in_chunk)

    avg_fill_px = total_cash_spent / executed_total if executed_total > 0 else 0
    return total_cash_spent, avg_fill_px

def main():
    print("--- Starting Data Production ---")
    kafka_producer()
    print("--- Data Production Finished ---")
    
    # --- End of Changes ---

    print("\n--- Starting Consumer and Backtest ---")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000  
    )

    print("Consuming snapshots from Kafka...")
    snapshots = []
    current_snapshot = {}
    last_timestamp = None

    for message in consumer:
        data = message.value
        timestamp = data['timestamp']

        if timestamp != last_timestamp and last_timestamp is not None:
            snapshots.append((last_timestamp, current_snapshot))
            current_snapshot = {}
        
        current_snapshot[data['publisher_id']] = data
        last_timestamp = timestamp

    if current_snapshot:
        snapshots.append((last_timestamp, current_snapshot))
    
    print(f"Collected {len(snapshots)} unique snapshots. Running simulations...")

    
    best_sor_cost = float('inf')
    best_sor_params = None
    optimized_results = {}

    for params in PARAM_GRID:
        total_cash, avg_px = run_sor_simulation(snapshots, params)
        if total_cash < best_sor_cost:
            best_sor_cost = total_cash
            best_sor_params = params
            optimized_results = {"total_cash": total_cash, "avg_fill_px": avg_px}
    
    best_ask_cash, best_ask_px = run_best_ask_simulation(snapshots)
    twap_cash, twap_px = run_twap_simulation(snapshots)
    vwap_cash, vwap_px = run_vwap_simulation(snapshots)
    
    final_output = {
        "best_parameters": best_sor_params,
        "optimized": optimized_results,
        "baselines": {
            "best_ask": {"total_cash": best_ask_cash, "avg_fill_px": best_ask_px},
            "twap": {"total_cash": twap_cash, "avg_fill_px": twap_px},
            "vwap": {"total_cash": vwap_cash, "avg_fill_px": vwap_px},
        },
        "savings_vs_baselines_bps": {
            "best_ask": ((best_ask_px - optimized_results.get('avg_fill_px', 0)) / best_ask_px * 10000) if best_ask_px > 0 else 0,
            "twap": ((twap_px - optimized_results.get('avg_fill_px', 0)) / twap_px * 10000) if twap_px > 0 else 0,
            "vwap": ((vwap_px - optimized_results.get('avg_fill_px', 0)) / vwap_px * 10000) if vwap_px > 0 else 0,
        }
    }
    
    print(json.dumps(final_output, indent=4))

if __name__ == "__main__":
    main()