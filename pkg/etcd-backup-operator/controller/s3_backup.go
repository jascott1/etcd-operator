package controller

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	api "github.com/coreos/etcd-operator/pkg/apis/etcd/v1beta2"
	"github.com/coreos/etcd-operator/pkg/backup"
	"github.com/coreos/etcd-operator/pkg/backup/backupapi"
	backupS3 "github.com/coreos/etcd-operator/pkg/backup/s3"
	"github.com/coreos/etcd-operator/pkg/cluster/backupstorage"
	"github.com/coreos/etcd-operator/pkg/util/constants"
)

const (
	tmp = "/tmp"
)

// TODO: remove this and use backend interface for other options (PV, Azure)
func (b *Backup) handleS3(clusterName string, s3 *api.S3Source) error {
	ctx, cancel := context.WithTimeout(context.Background(), constants.DefaultRequestTimeout)
	rc, version, rev, err := backup.GetSnap(ctx, b.kubecli, nil, b.namespace, clusterName)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to retrieve snapshot: (%v)", err)
	}

	awsDir, so, err := b.setupAWSConfig(s3.AWSSecret)
	if err != nil {
		return fmt.Errorf("failed to set up aws config: (%v)", err)
	}
	defer os.RemoveAll(awsDir)

	prefix := backupapi.ToS3Prefix(s3.Prefix, b.namespace, clusterName)
	s3Dir, be, err := b.makeS3Backend(so, prefix, s3.S3Bucket)
	if err != nil {
		return fmt.Errorf("failed to create s3 backend: (%v)", err)
	}
	defer os.RemoveAll(s3Dir)

	_, err = backup.WriteSnap(version, rev, be, rc)
	if err != nil {
		return fmt.Errorf("failed to write snap to s3 backend: (%v)", err)
	}
	return err
}

func (b *Backup) setupAWSConfig(secret string) (awsDir string, so *session.Options, err error) {
	awsDir, err = ioutil.TempDir(tmp, "")
	if err != nil {
		return "", nil, err
	}

	so, err = backupstorage.SetupAWSConfig(b.kubecli, b.namespace, secret, awsDir)
	if err != nil {
		return "", nil, err
	}

	return awsDir, so, nil
}

func (b *Backup) makeS3Backend(so *session.Options, prefix, bucket string) (s3Dir string, be *backup.S3Backend, err error) {
	s3cli, err := backupS3.NewFromSessionOpt(bucket, prefix, *so)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create aws cli: (%v)", err)
	}

	s3Dir, err = ioutil.TempDir(tmp, "")
	if err != nil {
		return "", nil, err
	}

	return s3Dir, backup.NewS3Backend(s3cli, s3Dir), nil
}
