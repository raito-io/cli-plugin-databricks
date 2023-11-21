package masks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashSha256Mask_Generate(t *testing.T) {
	mask := HashSha256Mask()

	policy, err := mask.Generate("maskname", "string", &MaskingBeneficiaries{
		Users:  []string{"user1", "user2"},
		Groups: []string{"group1", "group2"},
	})

	require.NoError(t, err)

	assert.Equal(t, MaskingPolicy("CREATE OR REPLACE FUNCTION maskname(val string)\nRETURN CASE\n\tWHEN current_user() IN ('user1', 'user2') THEN val\n\tWHEN is_account_group_member('group1') THEN val\n\tWHEN is_account_group_member('group2') THEN val\n\tELSE sha2(val, 256)\nEND;"), policy)
}
