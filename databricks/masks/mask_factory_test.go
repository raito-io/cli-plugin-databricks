package masks

import (
	"fmt"
	"testing"

	"github.com/raito-io/bexpression/utils"
	"github.com/stretchr/testify/assert"
)

func TestMaskFactory_CreateMask(t *testing.T) {
	type fields struct {
		maskGenerators map[string]MaskGenerator
	}
	type args struct {
		maskName      string
		columnType    string
		maskType      *string
		beneficiaries *MaskingBeneficiaries
	}
	tests := []struct {
		name              string
		fields            fields
		args              args
		wantPolicyName    string
		wantMaskingPolicy MaskingPolicy
		wantErr           assert.ErrorAssertionFunc
	}{
		{
			name: "no registered mask generators -> default mask generator is used",
			fields: fields{
				maskGenerators: map[string]MaskGenerator{},
			},
			args: args{
				maskName:   "mask_name",
				columnType: "string",
				maskType:   utils.Ptr("string"),
				beneficiaries: &MaskingBeneficiaries{
					Users:  []string{"user1", "user2"},
					Groups: []string{"group1", "group2"},
				},
			},
			wantPolicyName:    "mask_name_string",
			wantMaskingPolicy: MaskingPolicy("CREATE OR REPLACE FUNCTION mask_name_string(val string)\nRETURN CASE\n\tWHEN current_user() IN ('user1', 'user2') THEN val\n\tWHEN is_account_group_member('group1') THEN val\n\tWHEN is_account_group_member('group2') THEN val\n\tELSE '*****'\nEND;"),
			wantErr:           assert.NoError,
		},
		{
			name: "sha mask generator",
			fields: fields{
				maskGenerators: map[string]MaskGenerator{
					SHA256MaskId: HashSha256Mask(),
				},
			},
			args: args{
				maskName:   "sha_mask",
				columnType: "string",
				maskType:   utils.Ptr(SHA256MaskId),
				beneficiaries: &MaskingBeneficiaries{
					Users:  []string{"user1", "user2"},
					Groups: []string{"group1", "group2"},
				},
			},
			wantPolicyName:    "sha_mask_string",
			wantMaskingPolicy: MaskingPolicy("CREATE OR REPLACE FUNCTION sha_mask_string(val string)\nRETURN CASE\n\tWHEN current_user() IN ('user1', 'user2') THEN val\n\tWHEN is_account_group_member('group1') THEN val\n\tWHEN is_account_group_member('group2') THEN val\n\tELSE sha2(val, 256)\nEND;"),
			wantErr:           assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &MaskFactory{
				maskGenerators: tt.fields.maskGenerators,
			}
			got, got1, err := f.CreateMask(tt.args.maskName, tt.args.columnType, tt.args.maskType, tt.args.beneficiaries)
			if !tt.wantErr(t, err, fmt.Sprintf("CreateMask(%v, %v, %v, %v)", tt.args.maskName, tt.args.columnType, tt.args.maskType, tt.args.beneficiaries)) {
				return
			}
			assert.Equalf(t, tt.wantPolicyName, got, "CreateMask(%v, %v, %v, %v)", tt.args.maskName, tt.args.columnType, tt.args.maskType, tt.args.beneficiaries)
			assert.Equalf(t, tt.wantMaskingPolicy, got1, "CreateMask(%v, %v, %v, %v)", tt.args.maskName, tt.args.columnType, tt.args.maskType, tt.args.beneficiaries)
		})
	}
}
