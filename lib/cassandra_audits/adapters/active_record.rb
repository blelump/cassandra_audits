require 'active_record'
require 'cassandra_audits/auditor'
require 'cassandra_audits/adapters/active_record/audit'
require 'cassandra_audits/adapters/active_record/user_audit'
require 'cassandra_audits/adapters/active_record/study_plan_audit'
require 'cassandra_audits/adapters/active_record/module_audit'

module CassandraAudits::Auditor::ClassMethods
  def default_ignored_attributes
    [self.primary_key, inheritance_column]
  end
end

ActiveRecord::Base.send :include, CassandraAudits::Auditor

CassandraAudits.audit_class = CassandraAudits::Adapters::ActiveRecord::Audit
CassandraAudits.audit_scope = CassandraAudits::Adapters::ActiveRecord

require 'cassandra_audits/sweeper'