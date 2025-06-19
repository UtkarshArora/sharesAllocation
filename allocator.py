import numpy as np

def compute_cost(split, venues, order_size, lambda_over, lambda_under, theta_queue, fee=0.003, rebate=0.002):
    executed = 0
    cash_spent = 0.0
    
    for i in range(len(venues)):
        exec_qty = min(split[i], venues[i].get('ask_sz_00', 0))
        executed += exec_qty
        cash_spent += exec_qty * (venues[i].get('ask_px_00', 0) + fee)
        maker_rebate = max(split[i] - exec_qty, 0) * rebate
        cash_spent -= maker_rebate
    
    underfill = max(order_size - executed, 0)
    overfill = max(executed - order_size, 0)
    risk_penalty = theta_queue * (underfill + overfill)
    cost_penalty = lambda_under * underfill + lambda_over * overfill
    
    return cash_spent + risk_penalty + cost_penalty

def allocate_dp_corrected(order_size, venues, lambda_over, lambda_under, theta_queue, step=100):

    n_venues = len(venues)
    
    memo = {}
    
    def dp(venue_idx, remaining_shares):

        if venue_idx == n_venues:
            if remaining_shares == 0:
                return 0.0, []
            else:
                return float('inf'), []
        
        if (venue_idx, remaining_shares) in memo:
            return memo[(venue_idx, remaining_shares)]
        
        best_cost = float('inf')
        best_allocation = []
        
        venue = venues[venue_idx]
        max_venue_qty = min(remaining_shares, venue.get('ask_sz_00', 0))
        
        for qty in range(0, max_venue_qty + 1, step):
            future_cost, future_alloc = dp(venue_idx + 1, remaining_shares - qty)
            
            if future_cost != float('inf'):
                current_alloc = [qty] + future_alloc
                
                if venue_idx == 0:
                    total_cost = compute_cost(current_alloc, venues, order_size,
                                            lambda_over, lambda_under, theta_queue)
                else:
                    total_cost = future_cost
                
                if total_cost < best_cost:
                    best_cost = total_cost
                    best_allocation = current_alloc
        
        if max_venue_qty > 0 and max_venue_qty % step != 0:
            qty = max_venue_qty
            future_cost, future_alloc = dp(venue_idx + 1, remaining_shares - qty)
            
            if future_cost != float('inf'):
                current_alloc = [qty] + future_alloc
                
                if venue_idx == 0:
                    total_cost = compute_cost(current_alloc, venues, order_size,
                                            lambda_over, lambda_under, theta_queue)
                else:
                    total_cost = future_cost
                
                if total_cost < best_cost:
                    best_cost = total_cost
                    best_allocation = current_alloc
        
        memo[(venue_idx, remaining_shares)] = (best_cost, best_allocation)
        return best_cost, best_allocation
    
    cost, allocation = dp(0, order_size)
    
    if allocation:
        final_allocation = allocation + [0] * (n_venues - len(allocation))
        return final_allocation, cost
    else:
        return [0] * n_venues, float('inf')


def allocate(order_size, venues, lambda_over, lambda_under, theta_queue, step=100):
    return allocate_dp_corrected(order_size, venues, lambda_over, lambda_under, theta_queue, step)

def test_allocator():
    venues = [
        {'ask_px_00': 50.0, 'ask_sz_00': 1000},
        {'ask_px_00': 50.1, 'ask_sz_00': 2000},
        {'ask_px_00': 50.2, 'ask_sz_00': 1500}
    ]
    
    order_size = 1000
    lambda_over = 0.4
    lambda_under = 0.6
    theta_queue = 0.3
    
    allocation, cost = allocate(order_size, venues, lambda_over, lambda_under, theta_queue)
    
    print(f"Test allocation: {allocation}")
    print(f"Test cost: {cost}")
    print(f"Total allocated: {sum(allocation)}")
    
    assert sum(allocation) == order_size, f"Allocation sum {sum(allocation)} != order_size {order_size}"
    print("✓ Test passed!")

if __name__ == "__main__":
    test_allocator()