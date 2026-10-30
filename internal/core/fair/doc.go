// Package fair plans how a batch of leases is shared between tenants.
//
// The planner is a deficit round robin over the tenants that currently have
// work, in a fixed ring order. Each turn grants a tenant a quantum of jobs
// proportional to its weight, and a turn that a batch cuts short resumes at
// the next call. As a result a batch of any size, or the same jobs asked for
// one at a time, gives every tenant the same share, and a tenant's total
// never drifts from its weighted share by more than one round.
//
// The planner is pure: it holds no clock, no lock and no store. Its caller
// asks for a plan, fetches the jobs, and reports back how many each tenant
// really had.
package fair
