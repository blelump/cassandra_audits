require 'active_record'
require 'cassandra_audits/auditor'
require 'cassandra_audits/adapters/active_record/audit'

module CassandraAudits::Auditor::ClassMethods
  def default_ignored_attributes
    [self.primary_key, inheritance_column]
  end
end

ActiveRecord::Base.send :include, CassandraAudits::Auditor

CassandraAudits.audit_class = CassandraAudits::Adapters::ActiveRecord::Audit

require 'cassandra_audits/sweeper'