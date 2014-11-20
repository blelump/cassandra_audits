require 'cassandra_audits/adapters/active_record/user_info.rb'

module CassandraAudits
  class PartitionKeyNotSpecified < ::Exception; end

  class Sweeper < ActiveModel::Observer
    # observe CassandraAudits.audit_class
    # observe CassandraAudits::Adapters::ActiveRecord::StudyPlanAudit
    # observe CassandraAudits::Adapters::ActiveRecord::UserAudit
    observe CassandraAudits::Adapters::ActiveRecord::UserInfo

    def around(controller)
      begin
        self.controller = controller
        yield
      ensure
        self.controller = nil
      end
    end

    def before_create(audit)
      audit.audit_data ||= OpenStruct.new
      if controller.present?

        audit.audit_data.user_id ||= current_user.try(:id) || 0
        if superior_user.present?
          audit.audit_data.superior_id ||= superior_user.id
          if controller.respond_to?(:superior_user_data)
            audit.audit_data.superior_data ||= controller.superior_user_data.to_json
          end
        end
        audit.audit_data.remote_address = controller.try(:request).try(:ip) || ""
      end
    end

    [:current_user, :superior_user].each do |method_name|
      define_method method_name do
        if controller.respond_to?(CassandraAudits.send("#{method_name}_method"))
          controller.send(CassandraAudits.send("#{method_name}_method"))
        end
      end
    end

    def add_observer!(klass)
      super
      define_callback(klass)
    end

    def define_callback(klass)
      observer = self
      callback_meth = :"_notify_audited_sweeper"
      klass.send(:define_method, callback_meth) do
        observer.update(:before_create, self)
      end
      klass.send(:before_create, callback_meth)
    end

    def controller
      ::CassandraAudits.store[:current_controller]
    end

    def controller=(value)
      ::CassandraAudits.store[:current_controller] = value
    end
  end
end

if defined?(ActionController) and defined?(ActionController::Base)
  ActionController::Base.class_eval do
    around_filter CassandraAudits::Sweeper.instance
  end

end
