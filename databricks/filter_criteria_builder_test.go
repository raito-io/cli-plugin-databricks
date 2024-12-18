package databricks

import (
	"context"
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/bexpression"
	"github.com/raito-io/bexpression/base"
	"github.com/raito-io/bexpression/datacomparison"
	"github.com/stretchr/testify/assert"

	"cli-plugin-databricks/databricks/types"
)

func TestFilterCriteriaBuilder(t *testing.T) {
	type want struct {
		query     string
		arguments []types.ColumnReference
	}
	tests := []struct {
		name      string
		args      bexpression.DataComparisonExpression
		want      want
		wantError assert.ErrorAssertionFunc
	}{
		{
			name: "Literal binary expression",
			args: bexpression.DataComparisonExpression{
				Literal: ptr.Bool(true),
			},
			want: want{
				query:     "TRUE",
				arguments: nil,
			},
			wantError: assert.NoError,
		},
		{
			name: "Comparison binary expression - data object reference",
			args: bexpression.DataComparisonExpression{
				Comparison: &datacomparison.DataComparison{
					Operator: datacomparison.ComparisonOperatorGreaterThan,
					LeftOperand: datacomparison.Operand{
						Reference: &datacomparison.Reference{
							EntityType: datacomparison.EntityTypeDataObject,
							EntityID:   `{"fullName":"metastoreid.RAITO_DEMO.ORDERING.LINEITEM.QUANTITY","id":"JJGSpyjrssv94KPk9dNuI","type":"column"}`,
						},
					},
					RightOperand: datacomparison.Operand{
						Literal: &datacomparison.Literal{
							Int: ptr.Int(2020),
						},
					},
				},
			},
			want: want{
				query:     "(QUANTITY > 2020)",
				arguments: []types.ColumnReference{"QUANTITY"},
			},
			wantError: assert.NoError,
		},
		{
			name: "Comparison binary expression - column reference by name",
			args: bexpression.DataComparisonExpression{
				Comparison: &datacomparison.DataComparison{
					Operator: datacomparison.ComparisonOperatorGreaterThan,
					LeftOperand: datacomparison.Operand{
						Reference: &datacomparison.Reference{
							EntityType: datacomparison.EntityTypeColumnReferenceByName,
							EntityID:   `QUANTITY`,
						},
					},
					RightOperand: datacomparison.Operand{
						Literal: &datacomparison.Literal{
							Int: ptr.Int(2020),
						},
					},
				},
			},
			want: want{
				query:     "(QUANTITY > 2020)",
				arguments: []types.ColumnReference{"QUANTITY"},
			},
			wantError: assert.NoError,
		},
		{
			name: "aggregation expression",
			args: bexpression.DataComparisonExpression{
				Aggregator: &bexpression.DataComparisonAggregator{
					Operator: base.AggregatorOperatorAnd,
					Operands: []bexpression.DataComparisonExpression{
						{
							Literal: ptr.Bool(true),
						},
						{
							Aggregator: &bexpression.DataComparisonAggregator{
								Operator: base.AggregatorOperatorOr,
								Operands: []bexpression.DataComparisonExpression{
									{
										Comparison: &datacomparison.DataComparison{
											Operator: datacomparison.ComparisonOperatorEqual,
											LeftOperand: datacomparison.Operand{
												Reference: &datacomparison.Reference{
													EntityType: datacomparison.EntityTypeColumnReferenceByName,
													EntityID:   `STATE`,
												},
											},
											RightOperand: datacomparison.Operand{
												Literal: &datacomparison.Literal{
													Str: ptr.String("CA"),
												},
											},
										},
									},
									{
										UnaryExpression: &bexpression.DataComparisonUnaryExpression{
											Operator: base.UnaryOperatorNot,
											Operand: bexpression.DataComparisonExpression{
												Comparison: &datacomparison.DataComparison{
													Operator: datacomparison.ComparisonOperatorLessThan,
													LeftOperand: datacomparison.Operand{
														Reference: &datacomparison.Reference{
															EntityType: datacomparison.EntityTypeColumnReferenceByName,
															EntityID:   `QUANTITY`,
														},
													},
													RightOperand: datacomparison.Operand{
														Literal: &datacomparison.Literal{
															Int: ptr.Int(2000),
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			want: want{
				query: "(TRUE AND ((STATE = 'CA') OR (NOT (QUANTITY < 2000))))",
				arguments: []types.ColumnReference{
					"STATE",
					"QUANTITY",
				},
			},
			wantError: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filterCriteriaBuilder := NewFilterCriteriaBuilder()

			err := tt.args.Accept(context.Background(), filterCriteriaBuilder)
			if !tt.wantError(t, err) {
				return
			}

			query, arguments := filterCriteriaBuilder.GetQueryAndArguments()

			assert.Equal(t, tt.want.query, query)
			assert.ElementsMatch(t, tt.want.arguments, arguments.Slice())
		})
	}
}
