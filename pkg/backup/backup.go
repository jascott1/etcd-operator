// Copyright 2016 The etcd-operator Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	api "github.com/coreos/etcd-operator/pkg/apis/etcd/v1beta2"
	"github.com/coreos/etcd-operator/pkg/backup/abs"
	"github.com/coreos/etcd-operator/pkg/backup/backupapi"
	"github.com/coreos/etcd-operator/pkg/backup/env"
	"github.com/coreos/etcd-operator/pkg/backup/s3"
	"github.com/coreos/etcd-operator/pkg/util/constants"
	"github.com/coreos/etcd-operator/pkg/util/etcdutil"
	"github.com/coreos/etcd-operator/pkg/util/k8sutil"
	"github.com/coreos/etcd/clientv3"

	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	PVBackupV1 = "v1"

	maxRecentBackupStatusCount = 10
)

type Backup struct {
	kclient kubernetes.Interface

	clusterName   string
	namespace     string
	policy        api.BackupPolicy
	listenAddr    string
	etcdTLSConfig *tls.Config
	selfHosted    bool

	be backend

	backupNow chan chan backupNowAck

	// recentBackupStatus keeps the statuses of 'maxRecentBackupStatusCount' recent backups.
	recentBackupsStatus []backupapi.BackupStatus
}

func New(kclient kubernetes.Interface, clusterName, ns string, sp api.ClusterSpec, listenAddr string) (*Backup, error) {
	bdir := path.Join(constants.BackupMountDir, PVBackupV1, clusterName)
	// We created not only backup dir and but also tmp dir under it.
	// tmp dir is used to store intermediate snapshot files.
	// It will be no-op if target dir existed.
	tmpDir := path.Join(bdir, backupTmpDir)
	err := os.MkdirAll(tmpDir, 0700)
	if err != nil {
		return nil, err
	}

	var be backend
	switch sp.Backup.StorageType {
	case api.BackupStorageTypePersistentVolume, api.BackupStorageTypeDefault:
		be = &fileBackend{dir: bdir}
	case api.BackupStorageTypeS3:
		s3Prefix := ""
		if sp.Backup.S3 != nil {
			s3Prefix = sp.Backup.S3.Prefix
		}
		s3cli, err := s3.New(os.Getenv(env.AWSS3Bucket), backupapi.ToS3Prefix(s3Prefix, ns, clusterName))
		if err != nil {
			return nil, err
		}

		be = &s3Backend{
			dir: tmpDir,
			S3:  s3cli,
		}
	case api.BackupStorageTypeABS:
		absCli, err := abs.New(os.Getenv(env.ABSContainer),
			os.Getenv(env.ABSStorageAccount),
			os.Getenv(env.ABSStorageKey),
			path.Join(ns, clusterName))
		if err != nil {
			return nil, err
		}

		be = &absBackend{
			ABS: absCli,
		}
	default:
		return nil, fmt.Errorf("unsupported storage type: %v", sp.Backup.StorageType)
	}

	var tc *tls.Config
	if sp.TLS.IsSecureClient() {
		d, err := k8sutil.GetTLSDataFromSecret(kclient, ns, sp.TLS.Static.OperatorSecret)
		if err != nil {
			return nil, err
		}
		tc, err = etcdutil.NewTLSConfig(d.CertData, d.KeyData, d.CAData)
		if err != nil {
			return nil, err
		}
	}

	return &Backup{
		kclient:       kclient,
		clusterName:   clusterName,
		namespace:     ns,
		policy:        *sp.Backup,
		listenAddr:    listenAddr,
		be:            be,
		etcdTLSConfig: tc,
		selfHosted:    sp.SelfHosted != nil,

		backupNow: make(chan chan backupNowAck),
	}, nil
}

func (b *Backup) Run() {
	go b.startHTTP()

	lastSnapRev := b.getLatestBackupRev()
	interval := constants.DefaultSnapshotInterval
	if b.policy.BackupIntervalInSecond != 0 {
		interval = time.Duration(b.policy.BackupIntervalInSecond) * time.Second
	}

	go func() {
		if b.policy.MaxBackups == 0 {
			return
		}
		for {
			<-time.After(10 * time.Second)
			err := b.be.purge(b.policy.MaxBackups)
			if err != nil {
				logrus.Errorf("fail to purge backups: %v", err)
			}
		}
	}()

	for {
		var ackchan chan backupNowAck
		select {
		case <-time.After(interval):
		case ackchan = <-b.backupNow:
			logrus.Info("received a backup request")
		}

		rev, err := b.saveSnap(lastSnapRev)
		if err != nil {
			logrus.Errorf("failed to save snapshot: %v", err)
		}

		lastSnapRev = rev

		if ackchan != nil {
			ack := backupNowAck{err: err}
			if err == nil {
				ack.status = b.getLatestBackupStatus()
			}
			ackchan <- ack
		}
	}
}

// saveSnap saves the latest snapshot if the given current revision is greater lastSnapRev.
func (b *Backup) saveSnap(lastSnapRev int64) (int64, error) {
	logrus.Printf("saving backup for cluster (%s)", b.clusterName)
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), constants.DefaultRequestTimeout)
	rc, etcdVersion, rev, err := GetSnap(ctx, b.kclient, b.etcdTLSConfig, b.namespace, b.clusterName)
	cancel()
	defer rc.Close()
	if err != nil {
		return lastSnapRev, err
	}

	if rev <= lastSnapRev {
		logrus.Info("skipped creating new backup: no change since last time")
		return lastSnapRev, nil
	}

	n, err := WriteSnap(etcdVersion, rev, b.be, rc)
	if err != nil {
		err = fmt.Errorf("write snapshot failed: %v", err)
		return lastSnapRev, err
	}

	bs := backupapi.BackupStatus{
		CreationTime:     time.Now().Format(time.RFC3339),
		Size:             toMB(n),
		Version:          etcdVersion,
		Revision:         rev,
		TimeTookInSecond: int(time.Since(start).Seconds() + 1),
	}
	b.recentBackupsStatus = append(b.recentBackupsStatus, bs)
	if len(b.recentBackupsStatus) > maxRecentBackupStatusCount {
		b.recentBackupsStatus = b.recentBackupsStatus[1:]
	}

	logrus.Printf("saving backup for cluster (%s) succeeded", b.clusterName)
	return rev, nil
}

// WriteSnap writes the snapshot to the backend.
func WriteSnap(version string, rev int64, be backend, rc io.ReadCloser) (size int64, err error) {
	n, err := be.save(version, rev, rc)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// GetSnap gets the snapshot from the cluster member with the maximum revision.
func GetSnap(ctx context.Context, kclient kubernetes.Interface, tls *tls.Config, namespace, clusterName string) (rc io.ReadCloser, etcdVersion string, rev int64, err error) {
	pods, err := getRunningPods(kclient, namespace, clusterName)
	if err != nil {
		return nil, "", 0, fmt.Errorf("no running etcd pods found")
	}

	m, rev := getMemberWithMaxRev(pods, tls)
	if m == nil {
		return nil, "", 0, fmt.Errorf("no reachable member")
	}

	cfg := clientv3.Config{
		Endpoints:   []string{m.ClientURL()},
		DialTimeout: constants.DefaultDialTimeout,
		TLS:         tls,
	}
	etcdcli, err := clientv3.New(cfg)
	if err != nil {
		return nil, "", 0, fmt.Errorf("failed to create etcd client (%v)", err)
	}
	defer etcdcli.Close()

	resp, err := etcdcli.Maintenance.Status(ctx, m.ClientURL())
	if err != nil {
		return nil, "", 0, fmt.Errorf("failed to get member status (%v)", err)
	}

	ctx, cancel := context.WithTimeout(ctx, constants.DefaultSnapshotTimeout)
	defer cancel()
	rc, err = etcdcli.Maintenance.Snapshot(ctx)
	if err != nil {
		return nil, "", 0, fmt.Errorf("failed to receive snapshot (%v)", err)
	}
	return rc, resp.Version, rev, nil
}

func (b *Backup) getLatestBackupRev() int64 {
	// If there is any error, we just exit backup sidecar because we can't serve the backup any way.
	name, err := b.be.getLatest()
	if err != nil {
		logrus.Fatal(err)
	}
	if len(name) == 0 {
		return 0
	}
	rev, err := getRev(name)
	if err != nil {
		logrus.Fatal(err)
	}
	return rev
}

func (b *Backup) getLatestBackupStatus() backupapi.BackupStatus {
	return b.recentBackupsStatus[len(b.recentBackupsStatus)-1]
}

func getRunningPods(kclient kubernetes.Interface, namespace, clusterName string) ([]*v1.Pod, error) {
	podList, err := kclient.Core().Pods(namespace).List(k8sutil.ClusterListOpt(clusterName))
	if err != nil {
		return nil, err
	}
	var pods []*v1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.Phase == v1.PodRunning {
			pods = append(pods, pod)
		}
	}
	return pods, nil
}

func getMemberWithMaxRev(pods []*v1.Pod, tc *tls.Config) (*etcdutil.Member, int64) {
	var member *etcdutil.Member
	maxRev := int64(0)
	for _, pod := range pods {
		m := &etcdutil.Member{
			Name:         pod.Name,
			Namespace:    pod.Namespace,
			SecureClient: tc != nil,
		}
		cfg := clientv3.Config{
			Endpoints:   []string{m.ClientURL()},
			DialTimeout: constants.DefaultDialTimeout,
			TLS:         tc,
		}
		etcdcli, err := clientv3.New(cfg)
		if err != nil {
			logrus.Warningf("failed to create etcd client for pod (%v): %v", pod.Name, err)
			continue
		}
		defer etcdcli.Close()

		ctx, cancel := context.WithTimeout(context.Background(), constants.DefaultRequestTimeout)
		resp, err := etcdcli.Get(ctx, "/", clientv3.WithSerializable())
		cancel()
		if err != nil {
			logrus.Warningf("getMaxRev: failed to get revision from member %s (%s)", m.Name, m.ClientURL())
			continue
		}

		logrus.Infof("getMaxRev: member %s revision (%d)", m.Name, resp.Header.Revision)
		if resp.Header.Revision > maxRev {
			maxRev = resp.Header.Revision
			member = m
		}
	}
	return member, maxRev
}
