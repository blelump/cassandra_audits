require 'spec_helper'


describe DepartmentsController do
    
  describe "POST #create" do
      
    it "creates new user and an audit log" do
      post :create
      assigns[:user].should be_present
      cu = controller.current_user
      
      audits = controller.current_user.audits.where({:user_id => cu.id}).limit(10)
      audits.should be_present
      audits.first.action.should eq "create"
      audits.first.audited_changes.should be_present
    end
      
    it "deletes existing user and created an audit log" do
      user =  User.create(:name => "Foo", :department_id => controller.current_user.department_id)
      delete :destroy, :id => user.id
      cu = controller.current_user
      audits = controller.current_user.audits.where({:user_id => cu.id})
      
      User.all.should_not include(user)
      audits.should be_present
      audits.first.action.should eq "destroy"
      audits.first.audited_changes.should be_blank
    end
    
  end
end

