module CassandraAudits
  module Adapters
    module ActiveRecord
      class StudyPlanAudit < Audit

        def initialize(audit_data = {}, decode = false)
          super(audit_data, decode)
        end

        protected

        def table_name
          "study_plan_audits"
        end

        def partition_key
          { :study_plan_id => audit_data.delete_field(:associated_id) }
        end

      end
    end
  end
end
