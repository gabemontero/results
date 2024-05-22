package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/cel-go/cel"
	"github.com/tektoncd/pipeline/pkg/apis/pipeline"
	celenv "github.com/tektoncd/results/pkg/api/server/cel"
	"github.com/tektoncd/results/pkg/api/server/db/errors"
	"github.com/tektoncd/results/pkg/api/server/db/pagination"
	"github.com/tektoncd/results/pkg/api/server/v1alpha2/result"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	"github.com/tektoncd/results/pkg/api/server/db"
	"github.com/tektoncd/results/pkg/api/server/v1alpha2/auth"
	"github.com/tektoncd/results/pkg/api/server/v1alpha2/log"
	"github.com/tektoncd/results/pkg/api/server/v1alpha2/record"
	"github.com/tektoncd/results/pkg/apis/v1alpha2"
	"github.com/tektoncd/results/pkg/logs"
	pb "github.com/tektoncd/results/proto/v1alpha2/results_go_proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GetLog streams log record by log request
func (s *Server) GetLog(req *pb.GetLogRequest, srv pb.Logs_GetLogServer) error {
	parent, res, name, err := log.ParseName(req.GetName())
	if err != nil {
		s.logger.Error(err)
		return status.Error(codes.InvalidArgument, "Invalid Name")
	}

	if err := s.auth.Check(srv.Context(), parent, auth.ResourceLogs, auth.PermissionGet); err != nil {
		s.logger.Error(err)
		// unauthenticated status code and debug message produced by Check
		return err
	}

	rec, err := getRecord(s.db, parent, res, name)
	if err != nil {
		s.logger.Error(err)
		return err
	}
	// Check if the input record is referenced in any logs record in the result
	if rec.Type != v1alpha2.LogRecordType {
		rec, err = getLogRecord(s.db, parent, res, name)
		if err != nil {
			s.logger.Error(err)
			return err
		}
	}

	stream, object, err := log.ToStream(srv.Context(), rec, s.config)
	if err != nil {
		s.logger.Error(err)
		return status.Error(codes.Internal, "Error streaming log")
	}
	if object.Status.Size == 0 {
		s.logger.Errorf("no logs exist for %s", req.GetName())
		return status.Error(codes.NotFound, "Log doesn't exist")
	}

	writer := logs.NewBufferedHTTPWriter(srv, req.GetName(), s.config.LOGS_BUFFER_SIZE)
	if _, err = stream.WriteTo(writer); err != nil {
		s.logger.Error(err)
		return status.Error(codes.Internal, "Error streaming log")
	}
	_, err = writer.Flush()
	if err != nil {
		s.logger.Error(err)
		return status.Error(codes.Internal, "Error streaming log")
	}
	return nil
}

func getLogRecord(txn *gorm.DB, parent, result, name string) (*db.Record, error) {
	store := &db.Record{}
	q := txn.
		Where(&db.Record{Result: db.Result{Parent: parent, Name: result}}).
		Where("data -> 'spec' -> 'resource' ->> 'uid' =  ?", name).
		First(store)
	if err := errors.Wrap(q.Error); err != nil {
		return nil, err
	}
	return store, nil
}

// UpdateLog updates log record content
func (s *Server) UpdateLog(srv pb.Logs_UpdateLogServer) error {
	var name, parent, resultName, recordName string
	var bytesWritten int64
	var rec *db.Record
	var object *v1alpha2.Log
	var stream log.Stream

	startTime := time.Now()
	var endTimeFlush time.Time

	defer func() {
		if stream != nil {
			if err := stream.Flush(); err != nil {
				s.logger.Error(err)
			}
			endTimeFlush = time.Now()
			s.logger.Infof("GGM UpateLog after flush kind %s ns %s name %s result name %s parent %s resultName %s recordName %s time spent %s",
				object.Spec.Resource.Kind, object.Spec.Resource.Namespace, object.Spec.Resource.Name, name, parent, resultName, recordName, endTimeFlush.Sub(startTime).String())
		}
	}()
	for {
		obj := v1alpha2.Resource{}
		if object != nil {
			obj = object.Spec.Resource
		}
		// the underlying grpc stream RecvMsg method blocks until this receives a message or it is done,
		// with the client now setting a context deadline, if a timeout occurs, that should make this done/canceled; let's check to confirm
		//deadline, ok := srv.Context().Deadline()
		//if !ok {
		//	s.logger.Infof("UpdateLog called with no deadline: %#v", srv)
		//} else {
		//	s.logger.Infof("UpdateLog called with deadline: %s for %#v", deadline.String(), srv)
		//}
		recvStart := time.Now()
		recv, err := srv.Recv()
		// If we reach the end of the srv, we receive an io.EOF error
		if err != nil {
			return s.handleReturn(srv, rec, object, bytesWritten, err, startTime)
		}
		s.logger.Infof("GGM2 GRPC receive kind %s ns %s name %s time spent %s", obj.Kind, obj.Namespace, obj.Name, time.Now().Sub(recvStart).String())

		// Ensure that we are receiving logs for the same record
		if name == "" {
			name = recv.GetName()
			s.logger.Debugf("receiving logs for %s", name)
			parent, resultName, recordName, err = log.ParseName(name)
			if err != nil {
				return s.handleReturn(srv, rec, object, bytesWritten, err, startTime)
			}
			authStart := time.Now()
			if err := s.auth.Check(srv.Context(), parent, auth.ResourceLogs, auth.PermissionUpdate); err != nil {
				return s.handleReturn(srv, rec, object, bytesWritten, err, startTime)
			}
			s.logger.Infof("GGM3 RBAC check kind %s ns %s name %s time spent %s", obj.Kind, obj.Namespace, obj.Name, time.Now().Sub(authStart).String())
		}
		if name != recv.GetName() {
			err := fmt.Errorf("cannot put logs for multiple records in the same server")
			return s.handleReturn(srv,
				rec,
				object,
				bytesWritten,
				err,
				startTime)
		}

		if rec == nil {
			recStart := time.Now()
			rec, err = getRecord(s.db.WithContext(srv.Context()), parent, resultName, recordName)
			if err != nil {
				return s.handleReturn(srv, rec, object, bytesWritten, err, startTime)
			}
			s.logger.Infof("GGM4 get record kind %s ns %s name %s time spent %s", obj.Kind, obj.Namespace, obj.Name, time.Now().Sub(recStart).String())
		}

		if stream == nil {
			createStreamStart := time.Now()
			stream, object, err = log.ToStream(srv.Context(), rec, s.config)
			if err != nil {
				return s.handleReturn(srv, rec, object, bytesWritten, err, startTime)
			}
			obj = object.Spec.Resource
			s.logger.Infof("GGM5 create stream kind %s ns %s name %s time spent %s", obj.Kind, obj.Namespace, obj.Name, time.Now().Sub(createStreamStart).String())
		}

		readStart := time.Now()

		var labelKey string
		switch obj.Kind {
		case "TaskRun":
			labelKey = pipeline.TaskRunLabelKey
		case "PipelineRun":
			labelKey = pipeline.PipelineLabelKey
		}

		inMemWriteBufferStdout := bytes.NewBuffer(make([]byte, 0))

		lo := metav1.ListOptions{
			LabelSelector: fmt.Sprintf("%s=%s", labelKey, obj.Name),
		}
		var pods *corev1.PodList
		pods, err = s.k8s.CoreV1().Pods(obj.Namespace).List(srv.Context(), lo)
		if err != nil {
			return s.handleReturn(srv, rec, object, bytesWritten, err, startTime)
		}
		for _, pod := range pods.Items {
			// fyi gocritic complained about employing 'containers := append(pod.Spec.InitContainers, pod.Spec.Containers...)'
			containers := []corev1.Container{}
			copy(containers, pod.Spec.InitContainers)
			containers = append(containers, pod.Spec.Containers...)
			for _, container := range containers {
				pipelineTaskName := pod.Labels[pipeline.PipelineTaskLabelKey]
				taskName := pod.Labels[pipeline.TaskLabelKey]

				task := taskName
				if len(task) == 0 {
					task = pipelineTaskName
				}
				ba, podLogsErr := s.getPodLogs(srv.Context(), obj.Namespace, pod.Name, container.Name, labelKey, task)
				if podLogsErr != nil {
					return podLogsErr
				}
				inMemWriteBufferStdout.Write(ba)
			}
		}

		//buffer := bytes.NewBuffer(recv.GetData())
		//written, err := stream.ReadFrom(buffer)
		written, err := stream.ReadFrom(inMemWriteBufferStdout)
		bytesWritten += written
		s.logger.Infof("GGM6 read stream kind %s ns %s name %s time spent %s", obj.Kind, obj.Namespace, obj.Name, time.Now().Sub(readStart).String())

		return s.handleReturn(srv, rec, object, bytesWritten, err, startTime)
	}
}

func (s *Server) getPodLogs(ctx context.Context, ns, pod, container, labelKey, task string) ([]byte, error) {
	podLogOpts := corev1.PodLogOptions{
		Container: container,
	}
	req := s.k8s.CoreV1().Pods(ns).GetLogs(pod, &podLogOpts)
	podLogs, err := req.Stream(ctx)
	if err != nil {
		return nil, err
	}
	defer podLogs.Close()

	if err != nil && k8serrors.IsNotFound(err) {
		msg := fmt.Sprintf("error getting logs for pod %s container %s: %s", pod, container, err.Error())
		msgBytes := []byte(msg)
		return msgBytes, nil
	}
	if err != nil {
		return nil, err
	}
	rdr := bufio.NewReader(podLogs)
	buf := new(bytes.Buffer)
	for {
		var line []byte
		line, _, err = rdr.ReadLine()
		if err != nil && err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		str := ""
		stepName := strings.TrimPrefix(container, "step-")
		str = fmt.Sprintf("[%s : %s] %s\n", task, stepName, string(line))
		if labelKey == pipeline.TaskRunLabelKey {
			str = fmt.Sprintf("[%s] %s\n", stepName, string(line))
		}
		_, err = buf.Write([]byte(str))
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil

}

func (s *Server) handleReturn(srv pb.Logs_UpdateLogServer, rec *db.Record, log *v1alpha2.Log, written int64, returnErr error, startTime time.Time) error {
	// When the srv reaches the end, srv.Recv() returns an io.EOF error
	// Therefore we should not return io.EOF if it is received in this function.
	// Otherwise, we should return the original error and not mask any subsequent errors handling cleanup/return.

	defer func() {
		timeSpent := time.Now().Sub(startTime).String()
		prefix := ""
		if log != nil {
			prefix = prefix + fmt.Sprintf("kind %s ns %s name %s ",
				log.Spec.Resource.Kind,
				log.Spec.Resource.Namespace,
				log.Spec.Resource.Name)
		}
		if rec != nil {
			prefix = prefix + fmt.Sprintf("rec parent %s rec name %s ", rec.Parent, rec.Name)
		}
		if returnErr != nil {
			prefix = prefix + fmt.Sprintf("returnErr %s ", returnErr.Error())
		}
		msg := fmt.Sprintf("GGM UpdateLog after handleReturn %s time spent %s", prefix, timeSpent)
		s.logger.Info(msg)
	}()

	// If no database record or Log, return the original error
	if rec == nil || log == nil {
		return returnErr
	}
	apiRec := record.ToAPI(rec)

	apiRec.UpdateTime = timestamppb.Now()
	if written > 0 {
		log.Status.Size = written
	}
	data, err := json.Marshal(log)
	if err != nil {
		if !isNilOrEOF(returnErr) {
			return returnErr
		}
		return err
	}
	apiRec.Data = &pb.Any{
		Type:  rec.Type,
		Value: data,
	}

	_, err = s.UpdateRecord(srv.Context(), &pb.UpdateRecordRequest{
		Record: apiRec,
		Etag:   rec.Etag,
	})

	if err != nil {
		if !isNilOrEOF(returnErr) {
			return returnErr
		}
		return err
	}

	if returnErr == io.EOF || returnErr == nil {
		s.logger.Debugf("received %d bytes for %s", written, apiRec.GetName())
		return srv.SendAndClose(&pb.LogSummary{
			Record:        apiRec.Name,
			BytesReceived: written,
		})
	}
	return returnErr
}

func isNilOrEOF(err error) bool {
	return err == nil || err == io.EOF
}

// ListLogs returns list log records
func (s *Server) ListLogs(ctx context.Context, req *pb.ListRecordsRequest) (*pb.ListRecordsResponse, error) {
	if req.GetParent() == "" {
		return nil, status.Error(codes.InvalidArgument, "Parent missing")
	}
	parent, _, err := result.ParseName(req.GetParent())
	if err != nil {
		s.logger.Error(err)
		return nil, status.Error(codes.InvalidArgument, "Invalid Name")
	}
	if err := s.auth.Check(ctx, parent, auth.ResourceLogs, auth.PermissionList); err != nil {
		s.logger.Debug(err)
		// unauthenticated status code and debug message produced by Check
		return nil, err

	}

	userPageSize, err := pageSize(int(req.GetPageSize()))
	if err != nil {
		return nil, err
	}

	start, err := pageStart(req.GetPageToken(), req.GetFilter())
	if err != nil {
		return nil, err
	}

	sortOrder, err := orderBy(req.GetOrderBy())
	if err != nil {
		return nil, err
	}

	env, err := recordCEL()
	if err != nil {
		return nil, err
	}
	prg, err := celenv.ParseFilter(env, req.GetFilter())
	if err != nil {
		return nil, err
	}
	// Fetch n+1 items to get the next token.
	rec, err := s.getFilteredPaginatedSortedLogRecords(ctx, req.GetParent(), start, userPageSize+1, prg, sortOrder)
	if err != nil {
		return nil, err
	}

	// If we returned the full n+1 items, use the last element as the next page
	// token.
	var nextToken string
	if len(rec) > userPageSize {
		next := rec[len(rec)-1]
		var err error
		nextToken, err = pagination.EncodeToken(next.GetUid(), req.GetFilter())
		if err != nil {
			return nil, err
		}
		rec = rec[:len(rec)-1]
	}

	return &pb.ListRecordsResponse{
		Records:       rec,
		NextPageToken: nextToken,
	}, nil
}

// getFilteredPaginatedSortedLogRecords returns the specified number of results that
// match the given CEL program.
func (s *Server) getFilteredPaginatedSortedLogRecords(ctx context.Context, parent, start string, pageSize int, prg cel.Program, sortOrder string) ([]*pb.Record, error) {
	parent, resultName, err := result.ParseName(parent)
	if err != nil {
		return nil, err
	}

	rec := make([]*pb.Record, 0, pageSize)
	batcher := pagination.NewBatcher(pageSize, minPageSize, maxPageSize)
	for len(rec) < pageSize {
		batchSize := batcher.Next()
		dbrecords := make([]*db.Record, 0, batchSize)
		q := s.db.WithContext(ctx).Where("type = ?", v1alpha2.LogRecordType)
		q = q.Where("id > ?", start)
		// Specifying `-` allows users to read Records across Results.
		// See https://google.aip.dev/159 for more details.
		if parent != "-" {
			q = q.Where("parent = ?", parent)
		}
		if resultName != "-" {
			q = q.Where("result_name = ?", resultName)
		}
		if sortOrder != "" {
			q = q.Order(sortOrder)
		}
		q = q.Limit(batchSize).Find(&dbrecords)
		if err := errors.Wrap(q.Error); err != nil {
			return nil, err
		}

		// Only return results that match the filter.
		for _, r := range dbrecords {
			api := record.ToAPI(r)
			ok, err := record.Match(api, prg)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}

			// Change resource name to log format
			parent, resultName, recordName, err := record.ParseName(api.Name)
			if err != nil {
				return nil, err
			}
			api.Name = log.FormatName(result.FormatName(parent, resultName), recordName)

			rec = append(rec, api)
			if len(rec) >= pageSize {
				return rec, nil
			}
		}

		// We fetched fewer results than requested - this means we've exhausted all items.
		if len(dbrecords) < batchSize {
			break
		}

		// Set params for next batch.
		start = dbrecords[len(dbrecords)-1].ID
		batcher.Update(len(dbrecords), batchSize)
	}
	return rec, nil
}

// DeleteLog deletes a given record and the stored log.
func (s *Server) DeleteLog(ctx context.Context, req *pb.DeleteLogRequest) (*empty.Empty, error) {
	parent, res, name, err := log.ParseName(req.GetName())
	if err != nil {
		return nil, err
	}
	if err := s.auth.Check(ctx, parent, auth.ResourceLogs, auth.PermissionDelete); err != nil {
		return &empty.Empty{}, err
	}

	// Check in the input record exists in the database
	rec, err := getRecord(s.db, parent, res, name)
	if err != nil {
		return &empty.Empty{}, err
	}
	// Check if the input record is referenced in any logs record
	if rec.Type != v1alpha2.LogRecordType {
		rec, err = getLogRecord(s.db, parent, res, name)
		if err != nil {
			return &empty.Empty{}, err
		}
	}

	streamer, _, err := log.ToStream(ctx, rec, s.config)
	if err != nil {
		return nil, err
	}
	err = streamer.Delete()
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, errors.Wrap(s.db.WithContext(ctx).Delete(&db.Record{}, rec).Error)
}
