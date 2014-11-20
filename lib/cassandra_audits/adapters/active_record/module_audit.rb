module CassandraAudits
  module Adapters
    module ActiveRecord
      class ModuleAudit < Audit

        def initialize(audit_data = {}, decode = false)
          super(audit_data, decode)
        end

        protected

        def table_name
          "module_audits"
        end

        def partition_key
          { :module_id => audit_data.delete_field(:associated_id) }
        end

      end
    end
  end
end
