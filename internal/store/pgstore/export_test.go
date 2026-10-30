package pgstore

// The statements whose query plans a test inspects.
var (
	TenantLeaseSQL = tenantLeaseSQL
	FIFOLeaseSQL   = fifoLeaseSQL
	ReapSelectSQL  = reapSelectSQL
	PruneSQL       = pruneSQL
	ActivitySQL    = activitySQL
)
