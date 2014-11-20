module CassandraAudits
  module Adapters
    module ActiveRecord
      class UserAudit < Audit

        def initialize(audit_data = {}, decode = false)
          super(audit_data, decode)
        end

        protected

        def table_name
          "user_audits"
        end

        def partition_key
          if audit_data.associated_id.present?
            audit_data.delete_field(:associated_id)
          end
          {}
        end

      end
    end
  end
end
