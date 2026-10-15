package main

import (
	"testing"
)

func TestOperationProcessKindFromEnvironment(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    string
		wantErr bool
	}{
		{
			name:  "defaults to API mode",
			value: "",
			want:  "api",
		},
		{
			name:  "accepts explicit API mode",
			value: "api",
			want:  "api",
		},
		{
			name:  "accepts worker mode",
			value: "worker",
			want:  "worker",
		},
		{
			name:  "trims whitespace",
			value: " worker ",
			want:  "worker",
		},
		{
			name:  "ignores letter case",
			value: "WORKER",
			want:  "worker",
		},
		{
			name:    "rejects unknown mode",
			value:   "scheduler",
			wantErr: true,
		},
		{
			name:    "rejects unsupported service name",
			value:   "frontend",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("NEXVARIN_PROCESS", test.value)

			got, err := operationProcessKindFromEnvironment()

			if test.wantErr {
				if err == nil {
					t.Fatal("expected an invalid process mode error")
				}
				return
			}

			if err != nil {
				t.Fatalf("select process mode: %v", err)
			}

			if got != test.want {
				t.Errorf("process mode = %q, want %q", got, test.want)
			}
		})
	}
}
