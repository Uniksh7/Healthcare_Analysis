from pyspark.sql.functions import broadcast
class Analysis:
    def join_claims_eligibility(self,claims,elig):
        return claims.join(elig,'pid','left')
    
    def join_rx_eligibility(self,rx,elig):
        return rx.join(elig,'pid','left')
    
    def broadcast_join(self,claims,elig):
        return claims.join(broadcast(elig),'pid','left')