package models

// Permission codenames enforced by the depot node.
const (
	PermView    = "dms_depot.view"    // read documents, node state, sync status
	PermCapture = "dms_depot.capture" // buffer a new document
	PermManage  = "dms_depot.manage"  // retry documents, trigger sync sweeps
)

// PermissionDescriptor is one entry registered with iag-authentication at boot.
type PermissionDescriptor struct {
	Name        string
	Description string
}

// PermissionDescriptors is the catalogue this service registers.
func PermissionDescriptors() []PermissionDescriptor {
	return []PermissionDescriptor{
		{Name: PermView, Description: "View depot documents, node state and sync status"},
		{Name: PermCapture, Description: "Capture (buffer) documents at the depot"},
		{Name: PermManage, Description: "Retry documents and trigger sync sweeps"},
	}
}
