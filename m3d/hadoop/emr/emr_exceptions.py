from m3d.exceptions.m3d_exceptions import M3DException


class M3DAWSAPIException(M3DException):
    def __init__(self, *args, **kwargs):
        super(M3DAWSAPIException, self).__init__(*args, **kwargs)


class M3DEMRStepException(M3DAWSAPIException):
    def __init__(self, emr_cluster_id, emr_step_id, msg=None):
        self.emr_cluster_id = emr_cluster_id
        self.emr_step_id = emr_step_id

        if not msg:
            msg = "EMR Step exception: " + \
                  "emr_cluster_id='{}'; ".format(self.emr_cluster_id) + \
                  "emr_step_id='{}'.".format(self.emr_step_id)

        super(M3DEMRStepException, self).__init__(msg)


class M3DEMRStepTimeout(M3DEMRStepException):
    def __init__(self, emr_cluster_id, emr_step_id, msg=None):
        if not msg:
            msg = "EMR Step timed out: " + \
                  "emr_cluster_id='{}'; ".format(emr_cluster_id) + \
                  "emr_step_id='{}'.".format(emr_step_id)

        super(M3DEMRStepTimeout, self).__init__(emr_cluster_id, emr_step_id, msg)


class M3DAWSAPIRequestError(M3DAWSAPIException):
    def __init__(self, url, method, request_status, failure_stage, msg=None):
        self.url = url
        self.method = method
        self.request_status = request_status
        self.failure_stage = failure_stage

        if not msg:
            msg = "API request failed: " + \
                  "url='{}'; ".format(self.url) + \
                  "method='{}'; ".format(self.method) + \
                  "request_status='{}'; ".format(self.request_status) + \
                  "failure_stage='{}'.".format(self.failure_stage)

        super(M3DAWSAPIRequestError, self).__init__(msg)
