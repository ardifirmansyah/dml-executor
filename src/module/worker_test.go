package module

import (
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/jmoiron/sqlx"
)

func Test_worker_getStartEndID(t *testing.T) {
	type fields struct {
		db         *sqlx.DB
		jobType    string
		interval   int64
		batchLimit int64
	}
	type args struct {
		last int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int64
		want1  int64
	}{
		{
			name: "1-20",
			fields: fields{
				batchLimit: 20,
			},
			args: args{
				last: 0,
			},
			want:  1,
			want1: 20,
		},
		{
			name: "21-40",
			fields: fields{
				batchLimit: 20,
			},
			args: args{
				last: 20,
			},
			want:  21,
			want1: 40,
		},
		{
			name: "21-22",
			fields: fields{
				batchLimit: 1,
			},
			args: args{
				last: 20,
			},
			want:  21,
			want1: 21,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &worker{
				db:         tt.fields.db,
				jobType:    tt.fields.jobType,
				interval:   tt.fields.interval,
				batchLimit: tt.fields.batchLimit,
			}
			got, got1 := w.getStartEndID(tt.args.last)
			if got != tt.want {
				t.Errorf("getStartEndID() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("getStartEndID() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_worker_validateRowsAffected(t *testing.T) {
	type fields struct {
		db           *sqlx.DB
		logger       *logrus.Logger
		rowsAffected []int64
		tableName    string
		columnName   string
		jobType      string
		interval     int64
		batchLimit   int64
	}
	type args struct {
		rc int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "empty slice",
			fields: fields{
				rowsAffected: make([]int64, 0, 3),
			},
			args: args{
				rc: 0,
			},
			wantErr: false,
		},
		{
			name: "2+1 zero",
			fields: fields{
				rowsAffected: func() []int64 {
					a := make([]int64, 0, 3)
					a = append(a, 0)
					a = append(a, 0)
					return a
				}(),
			},
			args: args{
				rc: 0,
			},
			wantErr: true,
		},
		{
			name: "2 zero + 1 non zero",
			fields: fields{
				rowsAffected: func() []int64 {
					a := make([]int64, 0, 3)
					a = append(a, 0)
					a = append(a, 0)
					return a
				}(),
			},
			args: args{
				rc: 1,
			},
			wantErr: false,
		},
		{
			name: "test",
			fields: fields{
				rowsAffected: func() []int64 {
					a := make([]int64, 0, 3)
					a = append(a, 3)
					a = append(a, 0)
					a = append(a, 0)
					return a
				}(),
			},
			args: args{
				rc: 0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &worker{
				db:           tt.fields.db,
				logger:       tt.fields.logger,
				rowsAffected: tt.fields.rowsAffected,
				tableName:    tt.fields.tableName,
				columnName:   tt.fields.columnName,
				jobType:      tt.fields.jobType,
				interval:     tt.fields.interval,
				batchLimit:   tt.fields.batchLimit,
			}
			if err := w.validateRowsAffected(tt.args.rc); (err != nil) != tt.wantErr {
				t.Errorf("validateRowsAffected() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
